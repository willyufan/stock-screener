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

# ── Tushare（可选）──────────────────────────────────────────────────────────
_ts_pro = None
try:
    from config import TUSHARE_TOKEN
    if TUSHARE_TOKEN:
        import tushare as ts
        ts.set_token(TUSHARE_TOKEN)
        _ts_pro = ts.pro_api()
        logger.info("Tushare 初始化成功")
except Exception as _e:
    logger.info(f"Tushare 未启用: {_e}")

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

_DATA_DIR            = os.path.join(os.path.dirname(__file__), 'data')
QUALIFIED_CODES_FILE = os.path.join(_DATA_DIR, 'qualified_codes.json')

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


def _start_date() -> str:
    return (_now_cst() - timedelta(days=DAYS_LOOKBACK)).strftime('%Y%m%d')

def _end_date() -> str:
    return _now_cst().strftime('%Y%m%d')


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


def _fetch_a_hist_sina(symbol: str) -> pd.DataFrame | None:
    _limiter.wait()
    try:
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=_start_date(),
            end_date=_end_date(),
            adjust='qfq',
        )
        if df is None or df.empty:
            return None
        return _norm_hist(df)
    except Exception as e:
        logger.info(f"新浪历史失败 {symbol}: {e}")
        return None


def _fetch_a_hist_tushare(symbol: str) -> pd.DataFrame | None:
    if _ts_pro is None:
        return None
    _limiter.wait()
    try:
        df = _ts_pro.daily(ts_code=_a_to_ts(symbol),
                           start_date=_start_date(), end_date=_end_date())
        if df is None or df.empty:
            return None
        df['amount'] = df['amount'].astype(float) * 1000  # 千元→元
        return _norm_hist(df)
    except Exception as e:
        logger.debug(f"Tushare A股历史失败 {symbol}: {e}")
        return None


def _fetch_a_hist(symbol: str) -> pd.DataFrame | None:
    """优先级: Tushare Pro → 新浪"""
    df = _fetch_a_hist_tushare(symbol)
    if df is not None and len(df) >= 15:
        return df
    return _fetch_a_hist_sina(symbol)


# ════════════════════════════════════════════════════════════════
#  A股 实时行情（Tushare daily 主，新浪 spot 备）
# ════════════════════════════════════════════════════════════════

def _get_a_spot_tushare() -> tuple[pd.DataFrame, str]:
    """
    优先用 daily_basic（含精确总市值，需2000积分）
    回退 daily（免费，无市值）
    """
    if _ts_pro is None:
        return pd.DataFrame(), ''

    for offset in range(5):
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


def _get_a_spot() -> tuple[pd.DataFrame, str]:
    """
    优先级：
      1. Tushare daily_basic（精确市值）
      2. Tushare daily（无市值，免费）
      3. Sina spot（兜底，仅 Tushare 完全不可用时）
    """
    ts_df, ts_src = _get_a_spot_tushare()
    if not ts_df.empty:
        return ts_df, ts_src

    # Tushare 完全失败才用新浪
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

def _process_a_stock(row) -> dict | None:
    symbol  = str(row.get('代码', '')).zfill(6)
    name    = str(row.get('名称', row.get('name', '')))
    if not name or name == symbol:
        name = _name_cache.get(symbol, symbol)
    mktcap  = float(row.get('总市值', 0) or 0)

    hist = _fetch_a_hist(symbol)
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

def get_a_share_stocks(progress_cb=None, full_scan=False) -> list[dict]:
    """
    full_scan=False（普通刷新）：
        沪深300 + 中证500 成分股 + 上次全量扫描合格代码
    full_scan=True（全量扫描）：
        全市场，仅用成交额做初步预筛
    """
    logger.info(f"开始拉取 A股 ({'全量扫描' if full_scan else '快速刷新'})...")
    spot, source = _get_a_spot()

    if spot.empty:
        logger.error("A股 spot 数据获取失败")
        return []

    has_mktcap = source == 'tushare_basic' and '总市值' in spot.columns

    if has_mktcap:
        # 有精确市值：直接按100亿过滤，无需指数代理
        spot = spot[spot['总市值'] >= MIN_MARKET_CAP].copy()
        logger.info(f"精确市值过滤 >100亿 后: {len(spot)} 只")
    elif not full_scan:
        # 无市值 + 快速模式：CSI 800 + 历史合格代码 做代理
        universe = _get_index_codes()
        universe |= load_qualified_codes()
        spot = spot[spot['代码'].astype(str).str.zfill(6).isin(universe)].copy()
        logger.info(f"快速模式（CSI800 + 历史合格）: {len(spot)} 只")

    if full_scan and not has_mktcap:
        # 全量 + 无市值：宽松成交额预筛
        if '成交额' in spot.columns:
            spot = spot[spot['成交额'] >= MIN_AVG_VOLUME * 0.5].copy()
        logger.info(f"全量扫描预筛后: {len(spot)} 只")
    elif '成交额' in spot.columns:
        spot = spot[spot['成交额'] >= MIN_AVG_VOLUME * 0.3].copy()
        logger.info(f"成交额预筛后: {len(spot)} 只")

    results, total, done = [], len(spot), 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_process_a_stock, row): row.get('代码', '')
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
#  港股：沪港通成分股
# ════════════════════════════════════════════════════════════════

def _get_hk_connect_spot() -> tuple[list[tuple[str, str]], dict]:
    """
    返回 ([(代码, 名称), ...], {代码: spot_dict}) 的沪港通成分股列表
    spot_dict 包含当日实时行情字段
    """
    # 1. akshare stock_hk_ggt_components_em（首选，含实时行情）
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            codes = df['代码'].astype(str).str.zfill(5).tolist()
            names = df['名称'].astype(str).tolist()
            pairs = list(zip(codes, names))
            # Build spot map: {code: {price, change_pct, amount_yi, ...}}
            spot_map = {}
            for _, row in df.iterrows():
                code = str(row['代码']).zfill(5)
                try:
                    price  = float(row['最新价'])
                    chg    = float(row['涨跌幅'])
                    amt    = float(row['成交额']) / 1e8  # → 亿HKD
                    spot_map[code] = {
                        'current_price': price,
                        'change_pct':    round(chg, 2),
                        'today_amt_yi':  round(amt, 2),
                    }
                except Exception:
                    pass
            logger.info(f"[港股通] stock_hk_ggt_components_em 成功，共 {len(pairs)} 只")
            return pairs, spot_map
    except Exception as e:
        logger.warning(f"[港股通] stock_hk_ggt_components_em 失败: {e}")

    # 2. 尝试旧版函数名（兼容不同akshare版本）
    for fn_name in ('stock_hk_ggt_components_df', 'stock_em_hk_ggt_components',
                    'stock_hk_ggt_component'):
        try:
            fn = getattr(ak, fn_name, None)
            if fn is None:
                continue
            df = fn()
            if df is None or df.empty:
                continue
            code_col = next((c for c in df.columns if '代码' in c or 'code' in c.lower()), None)
            name_col = next((c for c in df.columns if '名称' in c or 'name' in c.lower()), None)
            if code_col:
                codes = df[code_col].astype(str).str.zfill(5).tolist()
                names = df[name_col].astype(str).tolist() if name_col else codes
                logger.info(f"[港股通] {fn_name} 成功，共 {len(codes)} 只")
                return list(zip(codes, names)), {}
        except Exception as e:
            logger.debug(f"港股通接口 {fn_name} 失败: {e}")

    # 3. 回退：Tushare hk_hold
    if _ts_pro is not None:
        try:
            for td_offset in range(5):
                td = (datetime.now() - timedelta(days=td_offset)).strftime('%Y%m%d')
                for exchange in ('SH', 'SZ'):
                    df = _ts_pro.hk_hold(trade_date=td, exchange=exchange)
                    if df is not None and not df.empty:
                        pairs = list(zip(
                            df['code'].astype(str).str.zfill(5),
                            df['name'].astype(str)
                        ))
                        logger.info(f"[港股通] Tushare hk_hold {td}/{exchange}，共 {len(pairs)} 只")
                        return pairs, {}
        except Exception as e:
            logger.debug(f"Tushare hk_hold 失败: {e}")

    logger.warning("港股通成分股获取失败，港股将无数据")
    return [], {}


def _get_hk_connect_codes() -> list[tuple[str, str]]:
    """兼容旧调用，仅返回 [(代码, 名称)]"""
    pairs, _ = _get_hk_connect_spot()
    return pairs


def _hk_to_ts(symbol: str) -> str:
    """港股代码转 Tushare 格式，如 '00700' → '00700.HK'"""
    return f"{symbol.lstrip('0').zfill(5)}.HK"


def _fetch_hk_hist(symbol: str) -> pd.DataFrame | None:
    _limiter.wait()

    # 1. Tushare Pro hk_daily（主）
    if _ts_pro is not None:
        try:
            df = _ts_pro.hk_daily(
                ts_code=_hk_to_ts(symbol),
                start_date=_start_date(), end_date=_end_date()
            )
            if df is not None and not df.empty:
                df['amount'] = df['amount'].astype(float)  # 已是 HKD 元
                result = _norm_hist(df)
                if len(result) >= 10:
                    return result
        except Exception as e:
            logger.debug(f"港股历史(Tushare) {symbol}: {e}")

    # 2. akshare stock_hk_hist（备）
    try:
        df = ak.stock_hk_hist(
            symbol=symbol, period='daily',
            start_date=_start_date(), end_date=_end_date(), adjust='qfq',
        )
        if df is not None and not df.empty:
            result = _norm_hist(df)
            if len(result) >= 10:
                return result
    except Exception as e:
        logger.debug(f"港股历史(akshare) {symbol}: {e}")

    # 3. yfinance（备2）
    if _HAS_YFINANCE:
        try:
            ticker = str(int(symbol.lstrip('0') or '0')).zfill(4)
            raw = yf.download(f"{ticker}.HK",
                              start=(datetime.now() - timedelta(days=DAYS_LOOKBACK)).strftime('%Y-%m-%d'),
                              progress=False, auto_adjust=True)
            if raw is not None and not raw.empty:
                df = raw.reset_index().rename(columns={
                    'Date': 'date', 'Open': 'open', 'High': 'high',
                    'Low': 'low', 'Close': 'close', 'Volume': 'volume',
                })
                df['amount']     = df['volume'].astype(float) * df['close'].astype(float)
                df['change_pct'] = df['close'].astype(float).pct_change() * 100
                df['date']       = df['date'].astype(str)
                return df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.debug(f"港股历史(yfinance) {symbol}: {e}")

    return None


def _process_hk_stock(code: str, name: str, spot: dict | None = None) -> dict | None:
    hist = _fetch_hk_hist(code)
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


def get_hk_connect_stocks(progress_cb=None) -> list[dict]:
    """获取沪港通成分股，不做量/市值过滤，直接做左侧/右侧分析"""
    logger.info("开始拉取 港股通 数据...")
    pairs, spot_map = _get_hk_connect_spot()
    if not pairs:
        return []

    logger.info(f"港股通成分股 {len(pairs)} 只，实时行情 {len(spot_map)} 只")
    results, total, done = [], len(pairs), 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_process_hk_stock, code, name, spot_map.get(code)): code
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
