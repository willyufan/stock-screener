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

import data_fetcher
import dcf_calc
import watchlist as wl_mod
import strategy as strat_mod

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

def _save_to_history(market: str, result: dict, scan_time: str, close_time: str | None):
    history = _load_history(market)
    scan_date = scan_time[:10]   # 'YYYY-MM-DD'
    snapshot = {
        'scan_time':     scan_time,
        'scan_date':     scan_date,
        'close_time':    close_time,
        'is_close_data': close_time is not None and scan_time[:16] >= close_time[:16],
        'label':         scan_date,   # 以日为单位展示
        'stats':         result['stats'],
        'data':          result,
    }
    # 去重：同一日期只保留最新一条
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
            stocks = data_fetcher.get_a_share_stocks(
                progress_cb=lambda mk, d, t: _progress_cb(market, d, t),
                full_scan=full_scan,
                date=date,
            )
        else:
            stocks = data_fetcher.get_hk_connect_stocks(
                progress_cb=lambda mk, d, t: _progress_cb(market, d, t),
                date=date,
            )

        if not stocks and date:
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
        close_time = _close_label(market)
        result     = _build_result(stocks)

        with _lock:
            _cache[market] = {
                'status':      'ready',
                'last_update': now,
                'close_time':  close_time,
                'progress':    None,
                'data':        result,
            }
        _save_cache()
        _save_to_history(market, result, now, close_time)
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


# ── API ──────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    with _lock:
        base = {
            'a_share': {
                'status':      _cache['a_share']['status'],
                'last_update': _cache['a_share']['last_update'],
                'close_time':  _cache['a_share'].get('close_time') or _close_label('a_share'),
                'progress':    _cache['a_share'].get('progress'),
            },
            'hk': {
                'status':      _cache['hk']['status'],
                'last_update': _cache['hk']['last_update'],
                'close_time':  _cache['hk'].get('close_time') or _close_label('hk'),
                'progress':    _cache['hk'].get('progress'),
            },
        }
    with _full_scan_lock:
        base['full_scan'] = {
            'running':    _full_scan['running'],
            'progress':   _full_scan['progress'],
            'last_run':   _full_scan['last_run'],
            'last_count': _full_scan['last_count'],
        }
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

    # 判断是否同日已扫描（仅在不指定历史日期时做此检查）
    close_time = _close_label(market)
    if not date_ts:
        today = _now_cst().strftime('%Y-%m-%d')
        if last_update and last_update[:10] == today:
            return jsonify({
                'status':      'already_latest',
                'last_update': last_update,
                'close_time':  close_time,
                'message':     f'今日已扫描（{last_update[:16]}），无需重复',
            })

    refresh_async(market, date=date_ts)
    return jsonify({
        'status':     'started',
        'close_time': close_time,
        'date':       date_label or _now_cst().strftime('%Y-%m-%d'),
    })


@app.route('/api/scan_history')
def api_scan_history():
    market = request.args.get('market', 'a_share')
    history = _load_history(market)
    # 只返回元信息，不含完整data
    return jsonify([{
        'scan_time':     h['scan_time'],
        'close_time':    h.get('close_time'),
        'is_close_data': h.get('is_close_data', False),
        'label':         h.get('label', h['scan_time']),
        'stats':         h['stats'],
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
    return jsonify({
        'status':      'ready',
        'last_update': snap['scan_time'],
        'close_time':  snap.get('close_time'),
        'stats':       data['stats'],
        'stocks':      stocks,
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


# ── 启动 ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    _load_cache()
    scheduler.start()

    if _cache['a_share']['data'] is None:
        logger.info("缓存为空，启动时自动拉取 A股 数据（快速模式）...")
        refresh_async('a_share')

    app.run(host='0.0.0.0', port=5005, debug=False)
