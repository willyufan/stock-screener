"""
A股/港股 左侧/右侧 股票筛选器
自动在每日收盘后刷新数据
"""
import json
import logging
import os
import threading
from datetime import datetime, timezone, timedelta
from typing import Any

_CST = timezone(timedelta(hours=8))
def _now_cst() -> datetime:
    return datetime.now(tz=_CST)

# ── 市场收盘时间 ──────────────────────────────────────────────────────────────
_CLOSE_TIMES = {
    'a_share': [(11, 30), (15, 0)],
    'hk':      [(12, 0),  (16, 0)],
}

def _get_last_close_time(market: str) -> datetime | None:
    """返回最近一次收盘时间（北京时间），仅限交易日"""
    now   = _now_cst()
    times = _CLOSE_TIMES.get(market, [(15, 0)])
    for days_back in range(8):
        day = now - timedelta(days=days_back)
        if day.weekday() >= 5:          # 周末跳过
            continue
        for h, m in reversed(times):
            t = day.replace(hour=h, minute=m, second=0, microsecond=0)
            if now > t:
                return t
    return None

def _close_label(market: str) -> str | None:
    """收盘时间字符串，用作 snapshot key"""
    t = _get_last_close_time(market)
    return t.strftime('%Y-%m-%d %H:%M') if t else None

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request


def _is_trading_hours() -> bool:
    """Return True if current Beijing time is within A-share market hours."""
    now = _now_cst()
    if now.weekday() >= 5:          # Saturday / Sunday
        return False
    t = now.hour * 60 + now.minute
    # Morning 9:25–11:35, Afternoon 12:55–15:05 (slight buffer)
    return (9 * 60 + 25 <= t <= 11 * 60 + 35) or (12 * 60 + 55 <= t <= 15 * 60 + 5)


def _get_stocks_for_auto_scan(market: str = 'a_share') -> list:
    """Gather all stocks from cache (realtime preferred) for auto strategy scan."""
    with _lock:
        entry = _cache.get(market, {})
        data = entry.get('realtime_data') or entry.get('data')
    if not data:
        return []
    return data.get('right', []) + data.get('left', []) + data.get('other', [])


def _auto_scan_positions():
    """Scheduled: every 5 min — feed positions scan to all active auto strategies."""
    if not _is_trading_hours():
        return
    stocks = _get_stocks_for_auto_scan('a_share')
    if not stocks:
        return
    scan_time = _now_cst().strftime('%Y-%m-%d %H:%M:%S')
    try:
        results = auto_engine.run_scan(stocks, scan_time, 'positions')
        logger.info(f"[自动策略] positions 扫描完成，股票{len(stocks)}只，实例{len(results)}个")
    except Exception as e:
        logger.exception(f"[自动策略] positions 扫描失败: {e}")


def _auto_scan_candidates():
    """Scheduled: every 15 min — feed candidates scan (entries + exits)."""
    if not _is_trading_hours():
        return
    stocks = _get_stocks_for_auto_scan('a_share')
    if not stocks:
        return
    scan_time = _now_cst().strftime('%Y-%m-%d %H:%M:%S')
    try:
        results = auto_engine.run_scan(stocks, scan_time, 'candidates')
        logger.info(f"[自动策略] candidates 扫描完成，股票{len(stocks)}只，实例{len(results)}个")
    except Exception as e:
        logger.exception(f"[自动策略] candidates 扫描失败: {e}")

import data_fetcher
import dcf_calc
import watchlist as wl_mod
import strategy as strat_mod
import backtest as bt_mod
from auto_strategy import engine as auto_engine
import auto_strategy.strategies.kelly_signal  # noqa: F401 — triggers @register

# ── 日志 ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CACHE_FILE        = os.path.join(os.path.dirname(__file__), 'cache.json')
_DATA_DIR         = os.path.join(os.path.dirname(__file__), 'data')
_HISTORY_MAX      = 20   # 最多保留快照数

os.makedirs(_DATA_DIR, exist_ok=True)

# ── 全局缓存 ─────────────────────────────────────────────────────────────────
_cache: dict[str, Any] = {
    'a_share': {'status': 'idle', 'data': None, 'last_update': None, 'progress': None},
    'hk':      {'status': 'idle', 'data': None, 'last_update': None, 'progress': None},
}
_lock = threading.Lock()

def _new_batch_state() -> dict:
    return {'status': 'idle', 'total': 0, 'done': 0, 'skipped': 0,
            'current_date': None, 'errors': []}

_batch_scan = {'a_share': _new_batch_state(), 'hk': _new_batch_state()}
_batch_lock = threading.Lock()

# 全量扫描状态
_full_scan: dict[str, Any] = {
    'running':   False,
    'progress':  {'done': 0, 'total': 0},
    'last_run':  None,
    'last_count': None,
}
_full_scan_lock = threading.Lock()

# ── 扫描历史 ──────────────────────────────────────────────────────────────────
def _history_file(market: str) -> str:
    return os.path.join(_DATA_DIR, f'scan_history_{market}.json')

def _load_history(market: str) -> list:
    try:
        with open(_history_file(market), encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def _compute_signal_changes(cur_data: dict, prev_data: dict) -> dict:
    """对比两次扫描数据，返回信号明显变化的股票列表。"""
    def build_map(data):
        all_s = data.get('right', []) + data.get('left', []) + data.get('other', [])
        return {s['code']: s for s in all_s}

    cur_map  = build_map(cur_data)
    prev_map = build_map(prev_data)
    buy_changes, sell_changes = [], []

    for code, cur in cur_map.items():
        prev_s = prev_map.get(code)
        if not prev_s:
            continue
        cur_sig   = cur.get('signal_strength', '')
        prev_side = prev_s.get('side', '')
        prev_sig  = prev_s.get('signal_strength', '')
        if cur_sig == prev_sig:
            continue
        # 任何信号升级到强烈买入（不限前一天 side）
        if cur_sig == '⚡强烈买入' and prev_sig != '⚡强烈买入':
            buy_changes.append({**cur, 'prev_side': prev_side, 'prev_signal': prev_sig})
        # 任何信号降级到强烈卖出
        elif cur_sig == '🔴强烈卖出' and prev_sig != '🔴强烈卖出':
            sell_changes.append({**cur, 'prev_side': prev_side, 'prev_signal': prev_sig})

    buy_changes.sort(key=lambda x: x.get('right_score', 0), reverse=True)
    sell_changes.sort(key=lambda x: x.get('left_score', 0), reverse=True)
    return {'buy': buy_changes, 'sell': sell_changes}


def _save_to_history(market: str, result: dict, scan_time: str, close_time: str | None,
                     is_realtime: bool = False):
    history = _load_history(market)
    scan_date = scan_time[:10]   # 'YYYY-MM-DD'

    # 找上一次不同日期的历史扫描，用于计算信号变化
    prev_day = next(
        (h for h in history if not h.get('is_realtime', False) and h.get('scan_date', '') != scan_date),
        None
    )
    sig_changes = {}
    if prev_day:
        try:
            sig_changes = _compute_signal_changes(result, prev_day.get('data', {}))
            sig_changes['prev_label'] = prev_day.get('label', prev_day.get('scan_date', ''))
        except Exception:
            pass

    snapshot = {
        'scan_time':      scan_time,
        'scan_date':      scan_date,
        'close_time':     close_time,
        'is_close_data':  close_time is not None and scan_time[:16] >= close_time[:16],
        'is_realtime':    is_realtime,
        'label':          scan_time[:16] if is_realtime else scan_date,
        'stats':          result['stats'],
        'signal_changes': sig_changes,
        'data':           result,
    }
    if is_realtime:
        # 实时扫描不按日期去重，每次都保留（最多30条实时记录混合历史）
        history.insert(0, snapshot)
    else:
        # 历史日期扫描：同一日期只保留最新一条
        history = [h for h in history if h.get('scan_date') != scan_date]
        history.insert(0, snapshot)
    history = history[:_HISTORY_MAX]
    with open(_history_file(market), 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False)


def _save_cache():
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(_cache, f, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")


def _all_cached_stocks() -> list:
    """从内存缓存中提取所有股票（A股 + 港股）"""
    stocks = []
    for market in ('a_share', 'hk'):
        data = _cache[market].get('data', {})
        for key in ('right', 'left', 'other'):
            stocks.extend(data.get(key, []))
    return stocks


def _load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            for k in ('a_share', 'hk'):
                if k in saved and saved[k].get('data'):
                    _cache[k]['data']        = saved[k]['data']
                    _cache[k]['last_update'] = saved[k].get('last_update')
                    _cache[k]['status']      = 'ready'
            logger.info("从磁盘加载缓存完成")
            # 同步更新自选池状态（解决重启后未更新问题）
            stocks = _all_cached_stocks()
            if stocks:
                wl_mod.update_all_statuses(stocks)
                logger.info(f"自选池状态已从缓存同步（{len(stocks)}只股票）")
        except Exception as e:
            logger.warning(f"读取缓存失败: {e}")


def _progress_cb(market: str, done: int, total: int):
    with _lock:
        _cache[market]['progress'] = {'done': done, 'total': total}


def _build_result(stocks: list) -> dict:
    right = sorted([s for s in stocks if s['side'] == '右侧'],
                   key=lambda x: x['right_score'], reverse=True)
    left  = sorted([s for s in stocks if s['side'] == '左侧'],
                   key=lambda x: x['left_score'],  reverse=True)
    other = sorted([s for s in stocks if s['side'] == '观望'],
                   key=lambda x: x['avg_amt_yi'],  reverse=True)
    return {
        'right': right,
        'left':  left,
        'other': other[:30],
        'stats': {
            'total':       len(stocks),
            'right_count': len(right),
            'left_count':  len(left),
            'other_count': len(other),
        }
    }


def _get_prev_day_data(market: str) -> dict | None:
    """返回最近一次历史日期（非实时）扫描的 data 字典，用于实时扫描确定目标股票"""
    history = _load_history(market)
    for h in history:
        if not h.get('is_realtime', False):
            return h.get('data')
    return None


def _refresh(market: str, full_scan: bool = False, date: str | None = None):
    """
    date: YYYYMMDD 指定交易日；None 表示最新
    """
    label = f"[{market}]{'[全量]' if full_scan else ''}" + (f"[{date}]" if date else '')
    logger.info(f"开始刷新 {label}...")
    with _lock:
        _cache[market]['status']   = 'loading'
        _cache[market]['progress'] = {'done': 0, 'total': 0}

    try:
        if market == 'a_share':
            if date:
                # 历史日期扫描：用 Tushare daily
                stocks = data_fetcher.get_a_share_stocks(
                    progress_cb=lambda mk, d, t: _progress_cb(market, d, t),
                    full_scan=full_scan,
                    date=date,
                )
            else:
                # 实时扫描：用 stk_mins，只扫左侧+观望+自选股
                prev_data = _get_prev_day_data(market)
                wl_codes  = [s['code'] for s in wl_mod.get_watchlist()
                             if s.get('market', 'A股') == 'A股']
                stocks = data_fetcher.get_a_share_stocks_realtime(
                    progress_cb=lambda mk, d, t: _progress_cb(market, d, t),
                    prev_history=prev_data,
                    watchlist_codes=wl_codes,
                )
        else:
            stocks = data_fetcher.get_hk_connect_stocks(
                progress_cb=lambda mk, d, t: _progress_cb(market, d, t),
                date=date,
            )

        if not stocks and (market == 'hk' or date):
            with _lock:
                _cache[market]['status'] = 'error'
                _cache[market]['error']  = f'{date} 无数据（非交易日或数据未就绪）'
            logger.warning(f"{label} 无数据，可能是非交易日")
            return []

        # 使用指定日期或实际收盘时间作为时间戳
        if date:
            now = f"{date[:4]}-{date[4:6]}-{date[6:]} 收盘"
        else:
            now = _now_cst().strftime('%Y-%m-%d %H:%M:%S')
        close_time  = _close_label(market)
        result      = _build_result(stocks)
        is_realtime = (date is None and market == 'a_share')

        with _lock:
            if is_realtime:
                # 实时扫描只更新状态和时间戳，不覆盖缓存数据（避免部分结果替换完整数据）
                _cache[market]['status']      = 'ready'
                _cache[market]['last_update'] = now
                _cache[market]['realtime_data'] = result
            else:
                _cache[market] = {
                    'status':      'ready',
                    'last_update': now,
                    'close_time':  close_time,
                    'progress':    None,
                    'data':        result,
                }
                _save_cache()
        if not is_realtime:
            _save_cache()
        _save_to_history(market, result, now, close_time, is_realtime=is_realtime)

        # 非实时日线扫描 → 自动追加到回测数据仓库
        if not is_realtime:
            try:
                bt_mod.update_store(market, result, now[:10], now)
            except Exception as e:
                logger.warning(f"更新回测仓库失败: {e}")
        logger.info(f"{label} 扫描完成 右侧:{result['stats']['right_count']} "
                    f"左侧:{result['stats']['left_count']} 观望:{result['stats']['other_count']}")

        # Update watchlist statuses
        try:
            wl_mod.update_all_statuses(stocks)
        except Exception as e:
            logger.warning(f"更新自选池状态失败: {e}")

        return stocks

    except Exception as e:
        logger.exception(f"{label} 刷新失败: {e}")
        with _lock:
            _cache[market]['status'] = 'error'
            _cache[market]['error']  = str(e)
        return []


def _get_trading_days(start_ts: str, end_ts: str) -> list[str]:
    """Return list of trading day strings (YYYY-MM-DD) between start_ts and end_ts inclusive."""
    from data_fetcher import _ts_pro
    try:
        if _ts_pro:
            start_d = start_ts.replace('-', '')
            end_d   = end_ts.replace('-', '')
            cal = _ts_pro.trade_cal(exchange='SSE', start_date=start_d, end_date=end_d, is_open='1')
            if cal is not None and not cal.empty:
                dates = sorted(cal['cal_date'].tolist())
                return [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in dates]
    except Exception as e:
        logger.warning(f"Tushare trade_cal 失败，回退到排除周末: {e}")
    # Fallback: exclude weekends
    from datetime import date, timedelta
    days = []
    cur = date.fromisoformat(start_ts)
    end = date.fromisoformat(end_ts)
    while cur <= end:
        if cur.weekday() < 5:
            days.append(str(cur))
        cur += timedelta(days=1)
    return days


def _do_batch_scan(market: str, dates: list[str]):
    bs = _batch_scan[market]
    with _batch_lock:
        bs.update({'status': 'running', 'total': len(dates), 'done': 0,
                   'skipped': 0, 'cached': 0, 'current_date': None, 'errors': []})

    # 预先加载已有历史，避免重复扫描
    history = _load_history(market)
    scanned_dates = {h.get('scan_date') for h in history if not h.get('is_realtime', False)}

    for date_ts in dates:
        # _refresh / Tushare 需要 YYYYMMDD 格式，_get_trading_days 返回 YYYY-MM-DD
        date_ymd = date_ts.replace('-', '')
        with _batch_lock:
            bs['current_date'] = date_ts

        # 已扫描过的日期直接跳过
        if date_ts in scanned_dates:
            with _batch_lock:
                bs['cached'] += 1
                bs['done'] += 1
            logger.info(f"[批量扫描] {market} {date_ts} 已有记录，跳过")
            continue

        try:
            _refresh(market, date=date_ymd)
            with _lock:
                if _cache[market]['status'] == 'error':
                    with _batch_lock:
                        bs['skipped'] += 1
                        bs['errors'].append(date_ts)
                    _cache[market]['status'] = 'ready'
                else:
                    scanned_dates.add(date_ts)  # 成功后加入已扫集合
        except Exception as e:
            logger.warning(f"[批量扫描] {market} {date_ts} 失败: {e}")
            with _batch_lock:
                bs['skipped'] += 1
                bs['errors'].append(date_ts)
        with _batch_lock:
            bs['done'] += 1
    with _batch_lock:
        bs['status'] = 'done'
        bs['current_date'] = None


def refresh_async(market: str, full_scan: bool = False, date: str | None = None):
    t = threading.Thread(target=_refresh, args=(market, full_scan, date), daemon=True)
    t.start()


# ── 全量扫描任务 ──────────────────────────────────────────────────────────────
def _do_full_scan():
    with _full_scan_lock:
        if _full_scan['running']:
            return
        _full_scan['running']  = True
        _full_scan['progress'] = {'done': 0, 'total': 0}

    logger.info("======= 全量扫描开始 =======")
    try:
        def progress_cb(mk, done, total):
            with _full_scan_lock:
                _full_scan['progress'] = {'done': done, 'total': total}
            _progress_cb('a_share', done, total)

        with _lock:
            _cache['a_share']['status']   = 'loading'
            _cache['a_share']['progress'] = {'done': 0, 'total': 0}

        stocks = data_fetcher.get_a_share_stocks(
            progress_cb=progress_cb, full_scan=True
        )

        # 保存合格代码，供后续快速刷新使用
        qualified = {s['code'] for s in stocks}
        data_fetcher.save_qualified_codes(qualified)

        now        = _now_cst().strftime('%Y-%m-%d %H:%M:%S')
        close_time = _close_label('a_share')
        result     = _build_result(stocks)

        with _lock:
            _cache['a_share'] = {
                'status':      'ready',
                'last_update': now,
                'close_time':  close_time,
                'progress':    None,
                'data':        result,
            }
        _save_cache()
        _save_to_history('a_share', result, now, close_time)

        # Update watchlist statuses
        try:
            wl_mod.update_all_statuses(stocks)
        except Exception as e:
            logger.warning(f"更新自选池状态失败: {e}")

        with _full_scan_lock:
            _full_scan['last_run']   = now
            _full_scan['last_count'] = len(stocks)

        logger.info(f"======= 全量扫描完成，共 {len(stocks)} 只 =======")

    except Exception as e:
        logger.exception(f"全量扫描失败: {e}")
        with _lock:
            _cache['a_share']['status'] = 'error'
    finally:
        with _full_scan_lock:
            _full_scan['running'] = False


# ── 定时任务 ─────────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
# A股：午盘 11:35，收盘 15:05（快速刷新，CSI800 + 历史合格）
scheduler.add_job(lambda: refresh_async('a_share'), 'cron',
                  hour=11, minute=35, day_of_week='mon-fri')
scheduler.add_job(lambda: refresh_async('a_share'), 'cron',
                  hour=15, minute=5,  day_of_week='mon-fri')
# 港股：午盘 12:05，收盘 16:05
scheduler.add_job(lambda: refresh_async('hk'), 'cron',
                  hour=12, minute=5,  day_of_week='mon-fri')
scheduler.add_job(lambda: refresh_async('hk'), 'cron',
                  hour=16, minute=5,  day_of_week='mon-fri')

# 自动交易策略：盘中每 5 分钟持仓扫描，每 15 分钟候选扫描
scheduler.add_job(_auto_scan_positions, 'cron',
                  hour='9-11,13-15', minute='*/5', day_of_week='mon-fri',
                  id='auto_positions', replace_existing=True)
scheduler.add_job(_auto_scan_candidates, 'cron',
                  hour='9-11,13-15', minute='*/15', day_of_week='mon-fri',
                  id='auto_candidates', replace_existing=True)


# ── API ──────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    with _lock:
        base = {
            'a_share': {
                'status':        _cache['a_share']['status'],
                'last_update':   _cache['a_share']['last_update'],
                'close_time':    _cache['a_share'].get('close_time') or _close_label('a_share'),
                'progress':      _cache['a_share'].get('progress'),
                'has_realtime':  _cache['a_share'].get('realtime_data') is not None,
            },
            'hk': {
                'status':        _cache['hk']['status'],
                'last_update':   _cache['hk']['last_update'],
                'close_time':    _cache['hk'].get('close_time') or _close_label('hk'),
                'progress':      _cache['hk'].get('progress'),
                'has_realtime':  False,
            },
        }
    with _full_scan_lock:
        base['full_scan'] = {
            'running':    _full_scan['running'],
            'progress':   _full_scan['progress'],
            'last_run':   _full_scan['last_run'],
            'last_count': _full_scan['last_count'],
        }
    base['batch_scan'] = {m: dict(_batch_scan[m]) for m in ('a_share', 'hk')}
    return jsonify(base)


@app.route('/api/stocks')
def api_stocks():
    market = request.args.get('market', 'a_share')
    side   = request.args.get('side', 'all')

    with _lock:
        entry  = _cache.get(market, {})
        status = entry.get('status', 'idle')
        if status != 'ready' or not entry.get('data'):
            return jsonify({'status': status, 'data': None,
                            'progress': entry.get('progress')})
        data = entry['data']

    if side == 'right':
        stocks = data['right']
    elif side == 'left':
        stocks = data['left']
    elif side == 'other':
        stocks = data['other']
    else:
        stocks = data['right'] + data['left'] + data['other']

    return jsonify({
        'status':      'ready',
        'last_update': entry['last_update'],
        'stats':       data['stats'],
        'stocks':      stocks,
    })


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    """兼容旧接口，同 /api/scan"""
    return api_scan()

@app.route('/api/scan', methods=['POST'])
def api_scan():
    body   = request.json or {}
    market = body.get('market', 'a_share')
    date_input = body.get('date', '')   # YYYY-MM-DD from frontend

    if market not in ('a_share', 'hk'):
        return jsonify({'error': 'invalid market'}), 400

    # 解析日期
    date_ts = None        # YYYYMMDD for Tushare
    date_label = None     # YYYY-MM-DD for display
    if date_input:
        try:
            d = datetime.strptime(date_input, '%Y-%m-%d')
            date_ts    = d.strftime('%Y%m%d')
            date_label = date_input
        except ValueError:
            return jsonify({'error': f'日期格式错误: {date_input}'}), 400

        # 不能扫描未来
        if d.date() > _now_cst().date():
            return jsonify({'error': '不能扫描未来日期'}), 400

        # 周末检查（若是周末，提示使用最近交易日）
        if d.weekday() >= 5:
            # 找最近的工作日
            last_td = d - timedelta(days=d.weekday() - 4)
            return jsonify({
                'status':  'non_trading',
                'message': f'{date_input} 为周末，请选择 {last_td.strftime("%Y-%m-%d")}（最近交易日）',
                'suggest': last_td.strftime('%Y-%m-%d'),
            })

    with _lock:
        if _cache[market]['status'] == 'loading':
            return jsonify({'status': 'already_loading'})
        last_update = _cache[market].get('last_update')

    close_time = _close_label(market)

    # 判断是否已扫描过（实时扫描检查今日，历史日期扫描检查 history）
    if not date_ts:
        today = _now_cst().strftime('%Y-%m-%d')
        if last_update and last_update[:10] == today:
            return jsonify({
                'status':      'already_latest',
                'last_update': last_update,
                'close_time':  close_time,
                'message':     f'今日已扫描（{last_update[:16]}），无需重复',
            })
    else:
        history = _load_history(market)
        existing = next(
            (h for h in history if not h.get('is_realtime', False) and h.get('scan_date') == date_label),
            None
        )
        if existing:
            return jsonify({
                'status':      'already_latest',
                'last_update': existing.get('scan_time', ''),
                'close_time':  close_time,
                'message':     f'{date_label} 已扫描过，无需重复',
            })

    refresh_async(market, date=date_ts)
    return jsonify({
        'status':     'started',
        'close_time': close_time,
        'date':       date_label or _now_cst().strftime('%Y-%m-%d'),
    })


@app.route('/api/scan_range', methods=['POST'])
def api_scan_range():
    data       = request.get_json(force=True) or {}
    market     = data.get('market', 'a_share')
    date_start = data.get('date_start', '')
    date_end   = data.get('date_end', '')
    if not date_start or not date_end:
        return jsonify({'error': 'date_start and date_end required'}), 400
    if _batch_scan[market]['status'] == 'running':
        return jsonify({'error': 'batch scan already running'}), 409
    dates = _get_trading_days(date_start, date_end)
    if not dates:
        return jsonify({'error': 'no trading days in range'}), 400
    t = threading.Thread(target=_do_batch_scan, args=(market, dates), daemon=True)
    t.start()
    return jsonify({'ok': True, 'trading_days': len(dates)})

@app.route('/api/scan_range/status')
def api_scan_range_status():
    market = request.args.get('market', 'a_share')
    with _batch_lock:
        return jsonify(dict(_batch_scan[market]))

@app.route('/api/trading_days')
def api_trading_days():
    market     = request.args.get('market', 'a_share')
    date_start = request.args.get('date_start', '')
    date_end   = request.args.get('date_end', '')
    if not date_start or not date_end:
        return jsonify({'count': 0, 'dates': []})
    dates = _get_trading_days(date_start, date_end)
    return jsonify({'count': len(dates), 'dates': dates})


@app.route('/api/scan_history')
def api_scan_history():
    market = request.args.get('market', 'a_share')
    history = _load_history(market)
    # 只返回元信息，不含完整data
    return jsonify([{
        'scan_time':         h['scan_time'],
        'close_time':        h.get('close_time'),
        'is_close_data':     h.get('is_close_data', False),
        'is_realtime':       h.get('is_realtime', False),
        'label':             h.get('label', h['scan_time']),
        'stats':             h['stats'],
        'has_sig_changes':   bool(h.get('signal_changes', {}).get('buy') or
                                  h.get('signal_changes', {}).get('sell')),
    } for h in history])


@app.route('/api/stocks_snapshot')
def api_stocks_snapshot():
    """按快照时间返回历史扫描数据"""
    market     = request.args.get('market', 'a_share')
    scan_time  = request.args.get('scan_time', '')
    history    = _load_history(market)
    snap = next((h for h in history if h['scan_time'] == scan_time), None)
    if not snap:
        return jsonify({'error': 'not found'}), 404
    data = snap['data']
    stocks = data['right'] + data['left'] + data['other']
    sc = snap.get('signal_changes', {})
    return jsonify({
        'status':         'ready',
        'last_update':    snap['scan_time'],
        'close_time':     snap.get('close_time'),
        'is_realtime':    snap.get('is_realtime', False),
        'stats':          data['stats'],
        'stocks':         stocks,
        'signal_changes': sc,
    })


@app.route('/api/full_scan', methods=['POST'])
def api_full_scan():
    with _full_scan_lock:
        if _full_scan['running']:
            return jsonify({'status': 'already_running'})
    t = threading.Thread(target=_do_full_scan, daemon=True)
    t.start()
    return jsonify({'status': 'started'})


# ── 自选池路由 ────────────────────────────────────────────────────────────────
@app.route('/api/watchlist')
def api_watchlist():
    return jsonify(wl_mod.get_watchlist())

@app.route('/api/watchlist', methods=['POST'])
def api_watchlist_add():
    d = request.json or {}
    ok = wl_mod.add_stock(d.get('code',''), d.get('name',''), d.get('market','a_share'), d.get('notes',''))
    return jsonify({'ok': ok})

@app.route('/api/watchlist/<code>', methods=['DELETE'])
def api_watchlist_remove(code):
    ok = wl_mod.remove_stock(code)
    return jsonify({'ok': ok})

@app.route('/api/watchlist/refresh', methods=['POST'])
def api_watchlist_refresh():
    """从当前缓存刷新自选池状态"""
    with _lock:
        stocks = _all_cached_stocks()
    if not stocks:
        return jsonify({'error': 'no scan data, please scan first'}), 400
    updated = wl_mod.update_all_statuses(stocks)
    return jsonify(updated)


@app.route('/api/signal_changes')
def api_signal_changes():
    """比较当前（实时或最新历史）与上一次不同日期的历史扫描，返回信号明显变化的股票"""
    market  = request.args.get('market', 'a_share')
    history = _load_history(market)

    def build_map(data):
        all_s = data.get('right', []) + data.get('left', []) + data.get('other', [])
        return {s['code']: s for s in all_s}

    # 优先用内存中的实时扫描结果作为"当前"
    with _lock:
        realtime_data = _cache.get(market, {}).get('realtime_data')
        rt_time       = _cache.get(market, {}).get('last_update', '')

    # 找最近一次历史日期扫描（非实时）
    prev_day_hist = next((h for h in history if not h.get('is_realtime', False)), None)

    if realtime_data and prev_day_hist:
        cur_map      = build_map(realtime_data)
        prev_map     = build_map(prev_day_hist.get('data', {}))
        cur_label    = f"实时 {rt_time[11:16]}" if len(rt_time) > 10 else '实时'
        prev_label   = prev_day_hist.get('label', '')
    else:
        # 回退：比较历史最新两条不同日期记录
        if len(history) < 2:
            return jsonify({'buy': [], 'sell': [], 'no_prev': True})
        latest = history[0]
        latest_date = latest.get('scan_date', '')
        prev = next((h for h in history[1:] if h.get('scan_date', '') != latest_date), None)
        if not prev:
            return jsonify({'buy': [], 'sell': [], 'no_prev': True})
        cur_map    = build_map(latest.get('data', {}))
        prev_map   = build_map(prev.get('data', {}))
        cur_label  = latest.get('label', latest_date)
        prev_label = prev.get('label', prev.get('scan_date', ''))

    buy_changes  = []
    sell_changes = []

    for code, cur in cur_map.items():
        prev_s = prev_map.get(code)
        if not prev_s:
            continue
        cur_sig   = cur.get('signal_strength', '')
        prev_side = prev_s.get('side', '')
        prev_sig  = prev_s.get('signal_strength', '')
        if cur_sig == prev_sig:
            continue

        if cur_sig == '⚡强烈买入' and prev_sig != '⚡强烈买入':
            buy_changes.append({**cur, 'prev_side': prev_side, 'prev_signal': prev_sig})
        elif cur_sig == '🔴强烈卖出' and prev_sig != '🔴强烈卖出':
            sell_changes.append({**cur, 'prev_side': prev_side, 'prev_signal': prev_sig})

    buy_changes.sort(key=lambda x: x.get('right_score', 0), reverse=True)
    sell_changes.sort(key=lambda x: x.get('left_score', 0), reverse=True)

    return jsonify({
        'buy':           buy_changes,
        'sell':          sell_changes,
        'current_label': cur_label,
        'prev_label':    prev_label,
    })


# ── 配置路由 ──────────────────────────────────────────────────────────────────
_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.py')

@app.route('/api/config', methods=['GET'])
def api_config_get():
    """读取当前 Tushare token 配置"""
    try:
        import importlib, config as _cfg
        importlib.reload(_cfg)
        return jsonify({
            'token_daily':  getattr(_cfg, 'TUSHARE_TOKEN_DAILY',  ''),
            'token_minute': getattr(_cfg, 'TUSHARE_TOKEN_MINUTE', ''),
            'ts_pro_ok':    data_fetcher._ts_pro  is not None,
            'ts_mins_ok':   data_fetcher._ts_mins is not None,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config', methods=['POST'])
def api_config_save():
    """保存 Tushare token 到 config.py 并热重载"""
    d = request.json or {}
    token_daily  = d.get('token_daily',  '').strip()
    token_minute = d.get('token_minute', '').strip()
    try:
        content = (
            "# 数据源配置\n"
            "# DAILY  token：daily / daily_basic / stock_basic 等日线接口\n"
            "# MINUTE token：stk_mins 分钟级实时接口\n"
            f'TUSHARE_TOKEN_DAILY  = "{token_daily}"\n'
            f'TUSHARE_TOKEN_MINUTE = "{token_minute}"\n'
        )
        with open(_CONFIG_FILE, 'w', encoding='utf-8') as f:
            f.write(content)
        data_fetcher._init_tushare()
        data_fetcher._load_stock_names()
        return jsonify({
            'ok':          True,
            'ts_pro_ok':   data_fetcher._ts_pro  is not None,
            'ts_mins_ok':  data_fetcher._ts_mins is not None,
        })
    except Exception as e:
        logger.exception(f"保存配置失败: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/stock_detail')
def api_stock_detail():
    """从缓存中查找单只股票的完整数据（含K线历史）"""
    code   = request.args.get('code', '')
    market = request.args.get('market', 'a_share')
    with _lock:
        entry = _cache.get(market, {})
        data  = entry.get('data')
    if not data:
        return jsonify({'error': 'no scan data'}), 404
    all_stocks = data.get('right', []) + data.get('left', []) + data.get('other', [])
    stock = next((s for s in all_stocks if s.get('code') == code), None)
    if not stock:
        return jsonify({'error': 'stock not found'}), 404
    return jsonify(stock)


# ── 策略路由 ──────────────────────────────────────────────────────────────────
@app.route('/api/strategies')
def api_strategies():
    return jsonify(strat_mod.list_strategies())

@app.route('/api/strategies', methods=['POST'])
def api_strategy_create():
    data = request.json or {}
    s = strat_mod.create_strategy(data)
    return jsonify(s)

@app.route('/api/strategies/<sid>', methods=['DELETE'])
def api_strategy_delete(sid):
    ok = strat_mod.delete_strategy(sid)
    return jsonify({'ok': ok})

@app.route('/api/strategies/<sid>/run', methods=['POST'])
def api_strategy_run(sid):
    """Run strategy against latest scan data"""
    market = (request.json or {}).get('market', 'a_share')
    with _lock:
        entry = _cache.get(market, {})
        data = entry.get('data')
    if not data:
        return jsonify({'error': 'no data'}), 400
    all_stocks = data.get('right', []) + data.get('left', []) + data.get('other', [])
    scan_date = _now_cst().strftime('%Y-%m-%d')
    result = strat_mod.run_on_scan(sid, all_stocks, scan_date)
    return jsonify(result)


# ── 自动策略路由 ──────────────────────────────────────────────────────────────

@app.route('/api/auto_strategies/available')
def api_auto_available():
    return jsonify(auto_engine.available_strategies())


@app.route('/api/auto_strategies')
def api_auto_list():
    return jsonify(auto_engine.list_instances())


@app.route('/api/auto_strategies', methods=['POST'])
def api_auto_create():
    d = request.json or {}
    result = auto_engine.create_instance(
        strategy_id=d.get('strategy_id', 'kelly_signal'),
        name=d.get('name', ''),
        params=d.get('params', {}),
    )
    if isinstance(result, str):
        return jsonify({'error': result}), 400
    return jsonify(result)


@app.route('/api/auto_strategies/<iid>')
def api_auto_get(iid):
    inst = auto_engine.get_instance(iid)
    if not inst:
        return jsonify({'error': 'not found'}), 404
    return jsonify(inst)


@app.route('/api/auto_strategies/<iid>', methods=['DELETE'])
def api_auto_delete(iid):
    ok = auto_engine.delete_instance(iid)
    return jsonify({'ok': ok})


@app.route('/api/auto_strategies/<iid>/reset', methods=['POST'])
def api_auto_reset(iid):
    result = auto_engine.reset_instance(iid)
    if not result:
        return jsonify({'error': 'not found'}), 404
    return jsonify(result)


@app.route('/api/auto_strategies/scan', methods=['POST'])
def api_auto_scan():
    """手动触发一次自动策略扫描"""
    body = request.json or {}
    scan_type = body.get('scan_type', 'candidates')
    stocks = _get_stocks_for_auto_scan('a_share')
    if not stocks:
        return jsonify({'error': '暂无股票数据，请先执行扫描'}), 400
    scan_time = _now_cst().strftime('%Y-%m-%d %H:%M:%S')
    results = auto_engine.run_scan(stocks, scan_time, scan_type)
    return jsonify({'ok': True, 'instances': results, 'stocks_count': len(stocks),
                    'scan_time': scan_time, 'scan_type': scan_type})


# ── DCF 路由 ──────────────────────────────────────────────────────────────────
@app.route('/dcf')
def dcf_page():
    return render_template('dcf.html')


@app.route('/api/dcf/list')
def api_dcf_list():
    return jsonify(dcf_calc.list_analyses())


@app.route('/api/dcf/save', methods=['POST'])
def api_dcf_save():
    data = request.json or {}
    # Compute result server-side
    try:
        result = dcf_calc.calculate_dcf(data.get('params', {}))
        data['result'] = result
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    aid = dcf_calc.save_analysis(data)
    return jsonify({'id': aid, 'result': result})


@app.route('/api/dcf/delete/<aid>', methods=['DELETE'])
def api_dcf_delete(aid):
    ok = dcf_calc.delete_analysis(aid)
    return jsonify({'ok': ok})


@app.route('/api/dcf/search')
def api_dcf_search():
    q = request.args.get('q', '').strip()
    market = request.args.get('market', 'a_share')
    if not q:
        return jsonify([])
    results = []
    # Search in name cache
    for code, name in data_fetcher._name_cache.items():
        if q in name or q in code:
            results.append({'code': code, 'name': name, 'market': 'A股'})
        if len(results) >= 20:
            break
    return jsonify(results[:20])


# ── 回测路由 ──────────────────────────────────────────────────────────────────

@app.route('/api/backtest/store_info')
def api_bt_store_info():
    market = request.args.get('market', 'a_share')
    info   = bt_mod.store_info(market)
    status = bt_mod.build_status()
    return jsonify({**info, 'build': status})


@app.route('/api/backtest/build_store', methods=['POST'])
def api_bt_build():
    """后台异步拉取历史数据并写入回测仓库"""
    body   = request.json or {}
    days   = int(body.get('days', 90))
    market = body.get('market', 'a_share')
    status = bt_mod.build_status()
    if status['running']:
        return jsonify({'status': 'already_running', 'build': status})
    bt_mod.build_store_async(days=days, market=market)
    return jsonify({'status': 'started', 'days': days, 'market': market})


@app.route('/api/backtest/list')
def api_bt_list():
    return jsonify(bt_mod.list_backtests())


@app.route('/api/backtest/run', methods=['POST'])
def api_bt_run():
    config = request.json or {}
    market = config.pop('market', 'a_share')
    result = bt_mod.run_backtest(config, market)
    if 'error' in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route('/api/backtest/<bt_id>')
def api_bt_get(bt_id):
    bt = bt_mod.get_backtest(bt_id)
    if not bt:
        return jsonify({'error': 'not found'}), 404
    return jsonify(bt)


@app.route('/api/backtest/<bt_id>', methods=['DELETE'])
def api_bt_delete(bt_id):
    ok = bt_mod.delete_backtest(bt_id)
    return jsonify({'ok': ok})


# ── 启动 ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    _load_cache()
    scheduler.start()

    if _cache['a_share']['data'] is None:
        logger.info("缓存为空，启动时自动拉取 A股 数据（快速模式）...")
        refresh_async('a_share')

    app.run(host='0.0.0.0', port=5005, debug=False)
