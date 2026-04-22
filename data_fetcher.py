"""
数据获取模块
数据源策略：
  A股 spot/成交额：Tushare daily（免费）> 新浪 stock_zh_a_spot
  A股 历史K线：    新浪 stock_zh_a_daily > Tushare pro.daily
  港股：           沪港通成分股列表 + yfinance / akshare 历史K线
"""
import os
import json
import urllib.request

# 清除所有代理，直连
for _k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy'):
    os.environ.pop(_k, None)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'
urllib.request.getproxies = lambda: {}

import requests
_OrigSession = requests.Session

class _NoProxySession(_OrigSession):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.trust_env = False
        self.proxies = {}
    def rebuild_proxies(self, prepared_request, proxies):
        return {}

requests.Session = _NoProxySession

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
import logging

# 始终用北京时间（UTC+8），不依赖本机时区
_CST = timezone(timedelta(hours=8))

def _now_cst() -> datetime:
    return datetime.now(tz=_CST)

from stock_analyzer import classify_stock

logger = logging.getLogger(__name__)

# ── 港股新浪 K线：共享 MiniRacer 实例，HTTP 并行，JS 解码串行 ─────────────────
_hk_sina_js_lock   = threading.Lock()
_hk_sina_js_engine = None

def _get_hk_js_engine():
    global _hk_sina_js_engine
    if _hk_sina_js_engine is None:
        try:
            from py_mini_racer import MiniRacer
            from akshare.stock.cons import hk_js_decode
            _hk_sina_js_engine = MiniRacer()
            _hk_sina_js_engine.eval(hk_js_decode)
        except Exception as e:
            logger.warning(f"MiniRacer 初始化失败: {e}")
            _hk_sina_js_engine = None
    return _hk_sina_js_engine

def _fetch_hk_sina(symbol: str) -> pd.DataFrame | None:
    """直连新浪港股接口，无频率限制，使用共享 MiniRacer 解码。"""
    try:
        from akshare.stock.cons import hk_sina_stock_hist_url
        url = hk_sina_stock_hist_url.format(symbol)
        r = requests.get(url, timeout=10)
        if r.status_code != 200 or '=' not in r.text:
            return None
        raw = r.text.split('=')[1].split(';')[0].replace('"', '')
        if not raw:
            return None
    except Exception as e:
        logger.debug(f"[新浪港股] HTTP {symbol}: {e}")
        return None
    with _hk_sina_js_lock:
        engine = _get_hk_js_engine()
        if engine is None:
            return None
        try:
            dict_list = engine.call('d', raw)
        except Exception as e:
            logger.debug(f"[新浪港股] JS解码 {symbol}: {e}")
            return None
    if not dict_list:
        return None
    df = pd.DataFrame(dict_list)
    if 'date' not in df.columns:
        return None
    df['date'] = pd.to_datetime(df['date']).dt.date.astype(str)
    for col in ('open', 'high', 'low', 'close', 'volume'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'amount' not in df.columns and 'volume' in df.columns and 'close' in df.columns:
        df['amount'] = df['volume'] * df['close']
    return df.sort_values('date').reset_index(drop=True)

# ── Tushare（可选，双 token）────────────────────────────────────────────────
# _ts_pro  : 日线接口（daily / daily_basic / stock_basic …）
# _ts_mins : 分钟接口（stk_mins）
_ts_pro  = None
_ts_mins = None

import tushare as ts   # noqa: E402 — 即使 token 为空也先 import，以便 reload 时可用


def _init_tushare():
    """
    从环境变量或 config.py 读取 Token，并（重新）初始化两个 pro_api 实例。
    优先级：环境变量 > config.py（生产环境可通过 env 注入，无需提交明文 token）
    """
    global _ts_pro, _ts_mins
    # 环境变量优先
    token_daily  = os.environ.get('TUSHARE_TOKEN_DAILY',  '')
    token_minute = os.environ.get('TUSHARE_TOKEN_MINUTE', '')
    # 未设置环境变量时回退到 config.py
    if not token_daily or not token_minute:
        try:
            import importlib, config as _cfg
            importlib.reload(_cfg)
            token_daily  = token_daily  or getattr(_cfg, 'TUSHARE_TOKEN_DAILY',  '') or ''
            token_minute = token_minute or getattr(_cfg, 'TUSHARE_TOKEN_MINUTE', '') or ''
        except Exception as e:
            logger.warning(f"读取 config 失败: {e}")

    if token_daily:
        try:
            ts.set_token(token_daily)
            _ts_pro = ts.pro_api()
            logger.info("Tushare [日线] 初始化成功")
        except Exception as e:
            logger.warning(f"Tushare [日线] 初始化失败: {e}")
            _ts_pro = None
    else:
        _ts_pro = None
        logger.info("Tushare [日线] token 未配置")

    if token_minute:
        try:
            ts.set_token(token_minute)
            _ts_mins = ts.pro_api()
            logger.info("Tushare [分钟] 初始化成功")
        except Exception as e:
            logger.warning(f"Tushare [分钟] 初始化失败: {e}")
            _ts_mins = None
    else:
        _ts_mins = None
        logger.info("Tushare [分钟] token 未配置")


_init_tushare()

# ── 名称 & 股本缓存 ──────────────────────────────────────────────────────────
_name_cache: dict[str, str] = {}    # '600000' -> '浦发银行'
_share_cache: dict[str, float] = {} # '600000' -> total_share in 万股

def _load_stock_names():
    """从 Tushare stock_basic 加载股票名称和总股本"""
    global _name_cache, _share_cache
    if _ts_pro is None:
        return
    try:
        df = _ts_pro.stock_basic(exchange='', list_status='L',
                                  fields='ts_code,name,total_share')
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                code = str(row['ts_code'])[:6]
                _name_cache[code] = str(row['name'])
                try:
                    _share_cache[code] = float(row['total_share'])
                except Exception:
                    pass
            logger.info(f"[Tushare] 加载股票名称: {len(_name_cache)} 条")
    except Exception as e:
        logger.debug(f"加载股票名称失败: {e}")

_load_stock_names()

# ── yfinance（可选）─────────────────────────────────────────────────────────
try:
    import yfinance as yf
    _HAS_YFINANCE = True
except ImportError:
    _HAS_YFINANCE = False

# ── 常量 ────────────────────────────────────────────────────────────────────
MIN_MARKET_CAP   = 100e8   # 100亿（用指数成分做代理）
MIN_AVG_VOLUME   = 1e8     # 1亿（30日均成交额）
DAYS_LOOKBACK    = 60
TOP10_DAYS       = 10
MAX_WORKERS      = 6
RATE_SLEEP       = 0.2
HK_WORKERS = 8  # cap: more than ~10 concurrent MiniRacer calls crashes libmini_racer

_DATA_DIR            = os.path.join(os.path.dirname(__file__), 'data')
QUALIFIED_CODES_FILE = os.path.join(_DATA_DIR, 'qualified_codes.json')
_HK_COMP_CACHE_FILE        = os.path.join(_DATA_DIR, 'hk_components_cache.json')
_HK_COMP_CACHE_MAX_AGE_DAYS = 7

os.makedirs(_DATA_DIR, exist_ok=True)


# ── 持久化：全量扫描合格代码 ─────────────────────────────────────────────────
def load_qualified_codes() -> set[str]:
    try:
        with open(QUALIFIED_CODES_FILE, encoding='utf-8') as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_qualified_codes(codes: set[str]):
    with open(QUALIFIED_CODES_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted(codes), f, ensure_ascii=False)
    logger.info(f"已保存 {len(codes)} 个合格代码到 {QUALIFIED_CODES_FILE}")


# ── 限速器 ──────────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, sleep=RATE_SLEEP):
        self._lock  = threading.Lock()
        self._count = 0
        self._sleep = sleep

    def wait(self):
        with self._lock:
            self._count += 1
            if self._count % 20 == 0:
                time.sleep(1.2)
            else:
                time.sleep(self._sleep)

_limiter = RateLimiter()


def _start_date(end: str | None = None) -> str:
    if end:
        base = datetime.strptime(end, '%Y%m%d')
    else:
        base = _now_cst()
    return (base - timedelta(days=DAYS_LOOKBACK)).strftime('%Y%m%d')

def _end_date(date: str | None = None) -> str:
    return date or _now_cst().strftime('%Y%m%d')


# ── 列名标准化 ───────────────────────────────────────────────────────────────
def _norm_hist(df: pd.DataFrame) -> pd.DataFrame:
    # 如果日期在 index 里（新浪 stock_zh_a_daily 的返回格式），先 reset
    if df.index.name in ('date', '日期', 'trade_date', 'Date'):
        df = df.reset_index()
    rename = {
        # 新浪 stock_zh_a_daily
        'outstanding_share': 'outstanding_share', 'turnover': 'turnover_rate',
        # 东财中文列名
        '日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low',
        '收盘': 'close', '成交量': 'volume', '成交额': 'amount',
        '涨跌幅': 'change_pct', '换手率': 'turnover_rate',
        # Tushare
        'trade_date': 'date', 'vol': 'volume', 'pct_chg': 'change_pct',
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    # 若列里还是没有 date，但 date 在 index 中
    if 'date' not in df.columns:
        df = df.reset_index()
        if df.columns[0] not in df.columns[1:]:
            df = df.rename(columns={df.columns[0]: 'date'})
    df['date'] = df['date'].astype(str)

    if 'amount' not in df.columns and 'volume' in df.columns and 'close' in df.columns:
        df['amount'] = df['volume'].astype(float) * df['close'].astype(float) * 100

    if 'change_pct' not in df.columns:
        df['change_pct'] = df['close'].astype(float).pct_change() * 100

    return df.sort_values('date').reset_index(drop=True)


# ════════════════════════════════════════════════════════════════
#  A股 历史K线
# ════════════════════════════════════════════════════════════════

def _a_to_ts(symbol: str) -> str:
    if symbol.startswith(('0', '3')):
        return f"{symbol}.SZ"
    if symbol.startswith('6'):
        return f"{symbol}.SH"
    return f"{symbol}.BJ"


def _fetch_a_hist_sina(symbol: str, end_date: str | None = None) -> pd.DataFrame | None:
    _limiter.wait()
    ed = _end_date(end_date)
    try:
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=_start_date(ed),
            end_date=ed,
            adjust='qfq',
        )
        if df is None or df.empty:
            return None
        return _norm_hist(df)
    except Exception as e:
        logger.info(f"新浪历史失败 {symbol}: {e}")
        return None


def _fetch_a_hist_tushare(symbol: str, end_date: str | None = None) -> pd.DataFrame | None:
    if _ts_pro is None:
        return None
    _limiter.wait()
    ed = _end_date(end_date)
    try:
        df = _ts_pro.daily(ts_code=_a_to_ts(symbol),
                           start_date=_start_date(ed), end_date=ed)
        if df is None or df.empty:
            return None
        df['amount'] = df['amount'].astype(float) * 1000  # 千元→元
        return _norm_hist(df)
    except Exception as e:
        logger.debug(f"Tushare A股历史失败 {symbol}: {e}")
        return None


def _fetch_a_hist(symbol: str, end_date: str | None = None) -> pd.DataFrame | None:
    """优先级: Tushare Pro → 新浪"""
    df = _fetch_a_hist_tushare(symbol, end_date)
    if df is not None and len(df) >= 15:
        return df
    return _fetch_a_hist_sina(symbol, end_date)


# ════════════════════════════════════════════════════════════════
#  A股 实时行情（Tushare daily 主，新浪 spot 备）
# ════════════════════════════════════════════════════════════════

def _get_a_spot_tushare(date: str | None = None) -> tuple[pd.DataFrame, str]:
    """
    优先用 daily_basic（含精确总市值，需2000积分）
    回退 daily（免费，无市值）
    date: 指定交易日 YYYYMMDD；None 则自动找最近交易日
    """
    if _ts_pro is None:
        return pd.DataFrame(), ''

    # 若指定日期，只尝试该日期；否则往前找5天
    offsets = [0] if date else range(5)

    for offset in offsets:
        if date:
            td = date
        else:
            td = (_now_cst() - timedelta(days=offset)).strftime('%Y%m%d')

        # 1. daily_basic + daily 合并（有市值、PE、成交额）
        try:
            db = _ts_pro.daily_basic(
                trade_date=td,
                fields='ts_code,total_mv,total_share,pe_ttm,pb'
            )
            dl = _ts_pro.daily(
                trade_date=td,
                fields='ts_code,close,amount,pct_chg'
            )
            if db is not None and dl is not None and not db.empty and not dl.empty:
                df = pd.merge(db, dl, on='ts_code', how='inner')
                df['代码']  = df['ts_code'].str[:6]
                df['名称']  = df['代码'].map(_name_cache).fillna('')
                df['总市值'] = df['total_mv'].astype(float) * 10000       # 万元→元
                df['成交额'] = df['amount'].astype(float) * 1000          # 千元→元
                df['涨跌幅'] = df['pct_chg'].astype(float)
                df['最新价'] = df['close'].astype(float)
                df['pe_ttm'] = pd.to_numeric(df['pe_ttm'], errors='coerce')
                df['pb']     = pd.to_numeric(df['pb'], errors='coerce')
                logger.info(f"[Tushare] daily_basic+daily 日期={td}，共 {len(df)} 只（含市值+PE）")
                return df, 'tushare_basic'
        except Exception as e:
            logger.info(f"Tushare daily_basic {td} 不可用（{e}），回退 daily")

        # 2. daily（无市值，免费）
        try:
            df = _ts_pro.daily(trade_date=td, fields='ts_code,close,vol,amount,pct_chg')
            if df is not None and not df.empty:
                df['代码']  = df['ts_code'].str[:6]
                df['成交额'] = df['amount'].astype(float) * 1000
                df['涨跌幅'] = df['pct_chg'].astype(float)
                df['最新价'] = df['close'].astype(float)
                logger.info(f"[Tushare] daily 日期={td}，共 {len(df)} 只（无市值）")
                return df, 'tushare'
        except Exception as e:
            logger.warning(f"Tushare daily {td} 异常: {e}")

    logger.warning("Tushare 全部失败，切换新浪")
    return pd.DataFrame(), ''


def _get_a_spot_sina() -> tuple[pd.DataFrame, str]:
    try:
        df = ak.stock_zh_a_spot()
        col_map = {'symbol': '代码', 'name': '名称',
                   'trade': '最新价', 'amount': '成交额', 'changepercent': '涨跌幅'}
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        if '代码' not in df.columns and df.index.name == 'symbol':
            df = df.reset_index().rename(columns={'symbol': '代码'})
        # 剥掉 sh/sz/bj 前缀，统一为纯6位数字代码
        df['代码'] = (df['代码'].astype(str)
                      .str.replace(r'^(sh|sz|bj)', '', regex=True)
                      .str.zfill(6))
        logger.info(f"[新浪] A股 spot 共 {len(df)} 只")
        return df, 'sina'
    except Exception as e:
        logger.warning(f"新浪 spot 失败: {e}")
        return pd.DataFrame(), ''


def _get_a_spot(date: str | None = None) -> tuple[pd.DataFrame, str]:
    """
    优先级：
      1. Tushare daily_basic（精确市值）
      2. Tushare daily（无市值，免费）
      3. Sina spot（兜底，仅 Tushare 完全不可用时，且不支持历史日期）
    date: YYYYMMDD，None 表示最新
    """
    ts_df, ts_src = _get_a_spot_tushare(date)
    if not ts_df.empty:
        return ts_df, ts_src

    if date:
        # 历史日期扫描，新浪只有实时数据，不适用
        logger.warning(f"[A股] 历史日期 {date} Tushare 无数据（非交易日？）")
        return pd.DataFrame(), 'no_data'

    # 当日实时兜底走新浪
    logger.warning("Tushare 无数据，回退新浪 spot")
    return _get_a_spot_sina()


# ── 指数成分股（市值代理）────────────────────────────────────────────────────

def _get_index_codes(index_ids=('000300', '000905')) -> set[str]:
    """沪深300 + 中证500 成分股，作为 >100亿 市值代理"""
    codes = set()

    for idx in index_ids:
        df = None

        # 方法1: csindex 接口
        try:
            df = ak.index_stock_cons_csindex(symbol=idx)
            if df is not None and not df.empty:
                logger.info(f"指数 {idx} csindex 列名: {df.columns.tolist()}")
        except Exception as e:
            logger.debug(f"csindex {idx} 失败: {e}")

        # 方法2: 东财接口（可能被墙则跳过）
        if df is None or df.empty:
            try:
                df = ak.index_stock_cons(symbol=idx)
                if df is not None and not df.empty:
                    logger.info(f"指数 {idx} index_stock_cons 列名: {df.columns.tolist()}")
            except Exception as e:
                logger.debug(f"index_stock_cons {idx} 失败: {e}")

        if df is None or df.empty:
            logger.warning(f"指数 {idx} 所有接口均失败")
            continue

        # 查找成分股代码列（排除 指数代码 自身）
        code_col = None
        # 优先精确匹配
        for preferred in ('成分券代码', '股票代码', '证券代码', 'con_code'):
            if preferred in df.columns:
                code_col = preferred
                break
        # 次选：含"代码"但不含"指数"
        if code_col is None:
            for c in df.columns:
                if '代码' in c and '指数' not in c:
                    code_col = c
                    break
        if code_col is None:
            code_col = df.columns[0]
            logger.warning(f"指数 {idx} 未找到代码列，使用第一列: {code_col}")

        batch = df[code_col].astype(str).str.strip().str.zfill(6).tolist()
        codes.update(batch)
        logger.info(f"指数 {idx} 成分: {len(batch)} 只")

    # Tushare 兜底（只在两个主接口都失败时才用）
    if len(codes) < 100 and _ts_pro is not None:
        logger.info("指数成分不足100，尝试 Tushare index_weight...")
        for ts_idx, name in (('000300.SH', '沪深300'), ('000905.SH', '中证500')):
            try:
                for offset in range(10):
                    td = (datetime.now() - timedelta(days=offset)).strftime('%Y%m%d')
                    df = _ts_pro.index_weight(index_code=ts_idx, trade_date=td)
                    if df is not None and not df.empty:
                        batch = df['con_code'].str[:6].tolist()
                        codes.update(batch)
                        logger.info(f"Tushare {name} 成分: {len(batch)} 只")
                        break
            except Exception as e:
                logger.debug(f"Tushare index_weight {ts_idx} 失败: {e}")

    logger.info(f"市值代理池（沪深300+中证500）合计: {len(codes)} 只")
    return codes


# ── 单只 A股 处理 ────────────────────────────────────────────────────────────

def _process_a_stock(row, end_date: str | None = None) -> dict | None:
    symbol  = str(row.get('代码', '')).zfill(6)
    name    = str(row.get('名称', row.get('name', '')))
    if not name or name == symbol:
        name = _name_cache.get(symbol, symbol)
    mktcap  = float(row.get('总市值', 0) or 0)

    hist = _fetch_a_hist(symbol, end_date)
    if hist is None or len(hist) < 15:
        return None

    # Compute market cap from shares × price if missing
    if mktcap == 0 and symbol in _share_cache:
        current_price = float(hist.iloc[-1]['close'])
        mktcap = _share_cache[symbol] * 10000 * current_price

    avg_amt = float(hist.tail(30)['amount'].mean())
    if avg_amt < MIN_AVG_VOLUME:
        return None

    # PE / PB from spot row（daily_basic 合并后才有）
    pe_ttm, pb = None, None
    try:
        pev = row.get('pe_ttm', None)
        if pev is not None and not pd.isna(pev) and float(pev) > 0:
            pe_ttm = round(float(pev), 1)
    except Exception:
        pass
    try:
        pbv = row.get('pb', None)
        if pbv is not None and not pd.isna(pbv) and float(pbv) > 0:
            pb = round(float(pbv), 2)
    except Exception:
        pass

    top10 = hist.tail(TOP10_DAYS)
    history = [
        {
            'date':       r['date'],
            'open':       round(float(r['open']),       3),
            'close':      round(float(r['close']),      3),
            'high':       round(float(r['high']),       3),
            'low':        round(float(r['low']),        3),
            'change_pct': round(float(r.get('change_pct', 0)), 2),
            'amount_yi':  round(float(r['amount']) / 1e8, 2),
        }
        for _, r in top10.iterrows()
    ]

    analysis = classify_stock(hist, pe_ttm=pe_ttm, pb=pb)
    return {
        'code':          symbol,
        'name':          name,
        'market':        'A股',
        'market_cap_yi': round(mktcap / 1e8, 1) if mktcap > 0 else None,
        'avg_amt_yi':    round(avg_amt / 1e8, 2),
        'current_price': round(float(hist.iloc[-1]['close']), 3),
        'change_pct':    round(float(hist.iloc[-1].get('change_pct', 0)), 2),
        'pe_ttm':        pe_ttm,
        'history':       history,
        **analysis,
    }


# ════════════════════════════════════════════════════════════════
#  A股 主入口（两种模式）
# ════════════════════════════════════════════════════════════════

def get_a_share_stocks(progress_cb=None, full_scan=False, date: str | None = None) -> list[dict]:
    """
    full_scan=False（普通刷新）：沪深300 + 中证500 成分股 + 上次全量合格代码
    full_scan=True（全量）：全市场
    date: YYYYMMDD 指定交易日（历史扫描），None 表示当日
    """
    label = f"{'全量扫描' if full_scan else '快速刷新'}" + (f"@{date}" if date else '')
    logger.info(f"开始拉取 A股 ({label})...")
    spot, source = _get_a_spot(date)

    if spot.empty:
        if source == 'no_data':
            logger.warning(f"A股 {date} 无交易数据（非交易日或数据未就绪）")
        else:
            logger.error("A股 spot 数据获取失败")
        return []

    has_mktcap = source == 'tushare_basic' and '总市值' in spot.columns

    if has_mktcap:
        spot = spot[spot['总市值'] >= MIN_MARKET_CAP].copy()
        logger.info(f"精确市值过滤 >100亿 后: {len(spot)} 只")
    elif not full_scan:
        universe = _get_index_codes()
        universe |= load_qualified_codes()
        spot = spot[spot['代码'].astype(str).str.zfill(6).isin(universe)].copy()
        logger.info(f"快速模式（CSI800 + 历史合格）: {len(spot)} 只")

    if full_scan and not has_mktcap:
        if '成交额' in spot.columns:
            spot = spot[spot['成交额'] >= MIN_AVG_VOLUME * 0.5].copy()
        logger.info(f"全量扫描预筛后: {len(spot)} 只")
    elif '成交额' in spot.columns:
        spot = spot[spot['成交额'] >= MIN_AVG_VOLUME * 0.3].copy()
        logger.info(f"成交额预筛后: {len(spot)} 只")

    results, total, done = [], len(spot), 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_process_a_stock, row, date): row.get('代码', '')
            for _, row in spot.iterrows()
        }
        for future in as_completed(futures):
            done += 1
            if progress_cb:
                progress_cb('a_share', done, total)
            if done % 100 == 0 or done == total:
                logger.info(f"A股历史进度: {done}/{total}，已符合 {len(results)} 只")
            r = future.result()
            if r:
                results.append(r)

    logger.info(f"A股筛选完成: {len(results)} 只符合条件")
    return results


# ════════════════════════════════════════════════════════════════
#  A股 实时扫描（stk_mins 模式）
#  目标：只对上次扫描的左侧+观望 + 自选股做实时信号更新
# ════════════════════════════════════════════════════════════════

# 限制并发 stk_mins 调用，避免触发 Tushare 频率限制
_mins_semaphore = threading.Semaphore(4)


def _fetch_today_bar_stk_mins(symbol: str) -> dict | None:
    """
    用 stk_mins 拉今日分钟数据，聚合成一根 today bar。
    返回 dict: {date, open, high, low, close, volume, amount, change_pct}
    或 None（无数据/非交易日/stk_mins 不可用）
    """
    if _ts_mins is None:
        return None
    ts_code = _a_to_ts(symbol)
    today   = _now_cst().strftime('%Y-%m-%d')
    try:
        with _mins_semaphore:
            df = _ts_mins.stk_mins(ts_code=ts_code, freq='1min')
    except Exception as e:
        logger.debug(f"stk_mins {symbol} 失败: {e}")
        return None

    if df is None or df.empty:
        return None

    df = df.sort_values('trade_time')
    # 只保留今日数据
    today_df = df[df['trade_time'].astype(str).str.startswith(today)]
    if today_df.empty:
        return None

    # 过滤掉成交量为 0 的占位 bar（非连续撮合时段）
    active = today_df[today_df['vol'] > 0]
    if active.empty:
        return None

    open_  = float(today_df.iloc[0]['open'])
    high_  = float(today_df['high'].max())
    low_   = float(today_df['low'].min())
    close_ = float(today_df.iloc[-1]['close'])
    vol_   = float(today_df['vol'].sum())
    amt_   = float(today_df['amount'].sum())
    return {
        'date':       today,
        'open':       open_,
        'high':       high_,
        'low':        low_,
        'close':      close_,
        'volume':     vol_,
        'amount':     amt_,
        'change_pct': None,   # 需要 yesterday close 来算，在 _process_a_stock_realtime 中填充
    }


def _process_a_stock_realtime(symbol: str, meta: dict) -> dict | None:
    """
    实时扫描：拉历史日线（截至昨日），追加今日 stk_mins bar，运行信号分析。
    meta: {名称, 总市值, pe_ttm, pb}
    """
    name   = meta.get('名称') or _name_cache.get(symbol, symbol)
    mktcap = float(meta.get('总市值', 0) or 0)
    pe_ttm = meta.get('pe_ttm')
    pb     = meta.get('pb')

    # 拉截至昨日的历史日线
    yesterday = (_now_cst() - timedelta(days=1)).strftime('%Y%m%d')
    hist = _fetch_a_hist(symbol, yesterday)
    if hist is None or len(hist) < 15:
        return None

    # 拉今日实时 bar
    today_bar = _fetch_today_bar_stk_mins(symbol)
    if today_bar:
        prev_close = float(hist.iloc[-1]['close'])
        if prev_close > 0:
            today_bar['change_pct'] = round((today_bar['close'] - prev_close) / prev_close * 100, 2)
        else:
            today_bar['change_pct'] = 0.0
        today_row = pd.DataFrame([today_bar])
        hist = pd.concat([hist, today_row], ignore_index=True)
        hist = hist.sort_values('date').reset_index(drop=True)

    if mktcap == 0 and symbol in _share_cache:
        mktcap = _share_cache[symbol] * 10000 * float(hist.iloc[-1]['close'])

    avg_amt = float(hist.tail(30)['amount'].mean())
    if avg_amt < MIN_AVG_VOLUME:
        return None

    top10   = hist.tail(TOP10_DAYS)
    history = [
        {
            'date':       r['date'],
            'open':       round(float(r['open']), 3),
            'close':      round(float(r['close']), 3),
            'high':       round(float(r.get('high', r['close'])), 3),
            'low':        round(float(r.get('low', r['close'])), 3),
            'change_pct': round(float(r.get('change_pct', 0) or 0), 2),
            'amount_yi':  round(float(r['amount']) / 1e8, 2),
        }
        for _, r in top10.iterrows()
    ]

    analysis = classify_stock(hist, pe_ttm=pe_ttm, pb=pb)
    last = hist.iloc[-1]
    return {
        'code':          symbol,
        'name':          name,
        'market':        'A股',
        'market_cap_yi': round(mktcap / 1e8, 1) if mktcap > 0 else None,
        'avg_amt_yi':    round(avg_amt / 1e8, 2),
        'current_price': round(float(last['close']), 3),
        'change_pct':    round(float(last.get('change_pct', 0) or 0), 2),
        'pe_ttm':        pe_ttm,
        'history':       history,
        **analysis,
    }


def get_a_share_stocks_realtime(progress_cb=None,
                                 prev_history: list | None = None,
                                 watchlist_codes: list | None = None) -> list[dict]:
    """
    实时扫描入口：只扫描上次历史扫描中左侧+观望的股票 + 自选股。
    用 stk_mins 获取当日实时 bar，追加到历史 K 线后再做信号分析。

    prev_history: 上一次（最近一次日期）的扫描 data（含 left/other 列表）
    watchlist_codes: 自选股代码列表（6位数字）
    """
    if _ts_mins is None:
        logger.warning("实时扫描需要 Tushare 分钟 token（stk_mins 权限）")
        return []

    # ── 构建扫描目标 ─────────────────────────────────────────────
    target_codes: set[str] = set()

    if prev_history:
        for s in prev_history.get('left', []):
            target_codes.add(str(s.get('code', '')).zfill(6))
        for s in prev_history.get('other', []):
            target_codes.add(str(s.get('code', '')).zfill(6))

    for code in (watchlist_codes or []):
        c = str(code).zfill(6)
        if c.isdigit():
            target_codes.add(c)

    # 只保留 A股代码（6位数字）
    target_codes = {c for c in target_codes if c.isdigit() and len(c) == 6}

    if not target_codes:
        logger.warning("实时扫描：没有目标股票（需先完成历史扫描或添加自选股）")
        return []

    # ── 获取上次 daily_basic（用于 PE/PB/市值）──────────────────
    meta_map: dict[str, dict] = {}
    try:
        for offset in range(5):
            td = (_now_cst() - timedelta(days=offset)).strftime('%Y%m%d')
            db = _ts_pro.daily_basic(
                trade_date=td,
                fields='ts_code,total_mv,total_share,pe_ttm,pb'
            )
            if db is not None and not db.empty:
                for _, r in db.iterrows():
                    code = str(r['ts_code'])[:6]
                    if code in target_codes:
                        meta_map[code] = {
                            '名称':   _name_cache.get(code, code),
                            '总市值': float(r.get('total_mv', 0) or 0) * 10000,
                            'pe_ttm': round(float(r['pe_ttm']), 1) if pd.notna(r.get('pe_ttm')) and float(r.get('pe_ttm', 0)) > 0 else None,
                            'pb':     round(float(r['pb']), 2)     if pd.notna(r.get('pb'))     and float(r.get('pb', 0)) > 0 else None,
                        }
                logger.info(f"[实时] daily_basic {td}: {len(meta_map)} 只有元数据")
                break
    except Exception as e:
        logger.warning(f"[实时] daily_basic 获取失败: {e}")

    # 没有 meta 的股票用空 dict
    for code in target_codes:
        if code not in meta_map:
            meta_map[code] = {'名称': _name_cache.get(code, code), '总市值': 0}

    # ── 并发处理 ─────────────────────────────────────────────────
    logger.info(f"[实时扫描] 目标 {len(target_codes)} 只（左侧+观望+自选股）")
    results, total, done = [], len(target_codes), 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_process_a_stock_realtime, code, meta_map.get(code, {})): code
            for code in target_codes
        }
        for future in as_completed(futures):
            done += 1
            if progress_cb:
                progress_cb('a_share', done, total)
            r = future.result()
            if r:
                results.append(r)

    logger.info(f"[实时扫描] 完成: {len(results)} 只有信号")
    return results


# ════════════════════════════════════════════════════════════════
#  港股：沪港通成分股
# ════════════════════════════════════════════════════════════════

def _load_hk_comp_cache() -> tuple[list[tuple[str, str]], bool]:
    """Load cached 港股通 component list. Returns (pairs, is_fresh)."""
    try:
        with open(_HK_COMP_CACHE_FILE, encoding='utf-8') as f:
            obj = json.load(f)
        pairs = [tuple(p) for p in obj.get('pairs', [])]
        saved_at = datetime.fromisoformat(obj['saved_at'])
        age_days = (datetime.now() - saved_at).total_seconds() / 86400
        return pairs, age_days < _HK_COMP_CACHE_MAX_AGE_DAYS
    except Exception:
        return [], False

def _get_hk_connect_spot() -> tuple[list[tuple[str, str]], dict]:
    """
    Returns (pairs, spot_map) where pairs = [(code, name), ...] for 沪深港通 stocks.
    Uses 7-day local cache; refreshes from network when stale.
    spot_map is always {} (spot prices fetched separately per stock).
    """
    # 0. Fresh cache → use directly
    cached_pairs, is_fresh = _load_hk_comp_cache()
    if is_fresh and cached_pairs:
        return cached_pairs, {}

    # 1. akshare stock_hk_ggt_components_em
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            code_col = next((c for c in df.columns if '代码' in c or 'code' in c.lower()), df.columns[0])
            name_col = next((c for c in df.columns if '名称' in c or 'name' in c.lower()), df.columns[1])
            pairs = [(str(r[code_col]).zfill(5), str(r[name_col])) for _, r in df.iterrows()]
            _save_hk_comp_cache(pairs)
            return pairs, {}
    except Exception as e:
        logger.warning(f"[港股通] akshare stock_hk_ggt_components_em 失败: {e}")

    # 2. akshare legacy functions
    for fn_name in ('stock_hk_ggt_constituents_em', 'stock_hk_hsgt_em'):
        try:
            df = getattr(ak, fn_name)()
            if df is not None and not df.empty:
                code_col = next((c for c in df.columns if '代码' in c or 'code' in c.lower()), df.columns[0])
                name_col = next((c for c in df.columns if '名称' in c or 'name' in c.lower()), df.columns[1])
                pairs = [(str(r[code_col]).zfill(5), str(r[name_col])) for _, r in df.iterrows()]
                _save_hk_comp_cache(pairs)
                return pairs, {}
        except Exception:
            continue

    # 3. Stale cache fallback
    if cached_pairs:
        logger.warning("[港股通] 网络获取失败，使用过期缓存")
        return cached_pairs, {}

    raise RuntimeError("无法获取港股通成分股列表，且无可用缓存")

def _save_hk_comp_cache(pairs: list[tuple[str, str]]):
    try:
        os.makedirs(os.path.dirname(_HK_COMP_CACHE_FILE), exist_ok=True)
        with open(_HK_COMP_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump({'saved_at': datetime.now().isoformat(), 'pairs': pairs}, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"[港股通] 缓存写入失败: {e}")


def _get_hk_connect_codes() -> list[tuple[str, str]]:
    """兼容旧调用，仅返回 [(代码, 名称)]"""
    pairs, _ = _get_hk_connect_spot()
    return pairs


def _hk_to_ts(symbol: str) -> str:
    """港股代码转 Tushare 格式，如 '00700' → '00700.HK'"""
    return f"{symbol.lstrip('0').zfill(5)}.HK"


def _fetch_hk_hist(symbol: str, end_date: str | None = None) -> pd.DataFrame | None:
    """Fetch HK stock daily K-line. symbol = 5-digit code e.g. '00700'."""
    today   = datetime.now().strftime('%Y%m%d')
    ed      = (end_date or today).replace('-', '')
    sd      = '20200101'
    ed_dash = f"{ed[:4]}-{ed[4:6]}-{ed[6:]}"
    sd_dash = f"{sd[:4]}-{sd[4:6]}-{sd[6:]}"

    # 1. 新浪港股（无频率限制，共享 MiniRacer）
    df = _fetch_hk_sina(symbol)
    if df is not None and not df.empty:
        df = df[df['date'] <= ed_dash]
        if not df.empty:
            return df

    # 2. Tushare hk_daily（备用）
    if _ts_pro:
        try:
            _limiter.wait()
            raw = _ts_pro.hk_daily(ts_code=f"{symbol}.HK", start_date=sd, end_date=ed)
            if raw is not None and not raw.empty:
                raw = raw.rename(columns={'trade_date': 'date', 'vol': 'volume', 'amount': 'amount'})
                raw['date'] = pd.to_datetime(raw['date']).dt.date.astype(str)
                for col in ('open', 'high', 'low', 'close', 'volume'):
                    if col in raw.columns:
                        raw[col] = pd.to_numeric(raw[col], errors='coerce')
                if 'amount' not in raw.columns and 'volume' in raw.columns and 'close' in raw.columns:
                    raw['amount'] = raw['volume'] * raw['close']
                return raw.sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.debug(f"[Tushare hk_daily] {symbol}: {e}")

    return None


def _process_hk_stock(code: str, name: str, spot: dict | None = None, end_date: str | None = None) -> dict | None:
    hist = _fetch_hk_hist(code, end_date)
    if hist is None or len(hist) < 10:
        return None

    top10 = hist.tail(TOP10_DAYS)
    history = [
        {
            'date':       r['date'],
            'open':       round(float(r['open']),       3),
            'close':      round(float(r['close']),      3),
            'high':       round(float(r['high']),       3),
            'low':        round(float(r['low']),        3),
            'change_pct': round(float(r.get('change_pct', 0)), 2),
            'amount_yi':  round(float(r['amount']) / 1e8, 2),
        }
        for _, r in top10.iterrows()
    ]
    avg_amt = float(hist.tail(30)['amount'].mean())
    analysis = classify_stock(hist)

    # 优先使用实时行情里的价格和涨跌幅
    if spot:
        current_price = spot.get('current_price') or round(float(hist.iloc[-1]['close']), 3)
        change_pct    = spot.get('change_pct')    if spot.get('change_pct') is not None \
                        else round(float(hist.iloc[-1].get('change_pct', 0)), 2)
    else:
        current_price = round(float(hist.iloc[-1]['close']), 3)
        change_pct    = round(float(hist.iloc[-1].get('change_pct', 0)), 2)

    return {
        'code':          code,
        'name':          name,
        'market':        '港股',
        'market_cap_yi': None,
        'avg_amt_yi':    round(avg_amt / 1e8, 2),
        'current_price': current_price,
        'change_pct':    change_pct,
        'history':       history,
        **analysis,
    }


def get_hk_connect_stocks(progress_cb=None, date: str | None = None) -> list[dict]:
    """获取沪港通成分股，不做量/市值过滤，直接做左侧/右侧分析
    date: YYYYMMDD 历史日期；None 则用当日实时行情
    """
    logger.info("开始拉取 港股通 数据" + (f" @{date}" if date else "") + "...")
    pairs, spot_map = _get_hk_connect_spot()
    if not pairs:
        return []

    # 历史日期扫描时不使用实时spot价格（用K线末尾价替代）
    if date:
        spot_map = {}

    logger.info(f"港股通成分股 {len(pairs)} 只，实时行情 {len(spot_map)} 只")
    results, total, done = [], len(pairs), 0

    with ThreadPoolExecutor(max_workers=HK_WORKERS) as pool:
        futures = {
            pool.submit(_process_hk_stock, code, name, spot_map.get(code), date): code
            for code, name in pairs
        }
        for future in as_completed(futures):
            done += 1
            if progress_cb:
                progress_cb('hk', done, total)
            r = future.result()
            if r:
                results.append(r)

    logger.info(f"港股通处理完成: {len(results)} 只有数据")
    return results
