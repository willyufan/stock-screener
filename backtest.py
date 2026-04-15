"""
回测引擎
════════════════════════════════════════════════════════
架构：
  · 独立回测数据仓库（backtest_store_{market}.json，最多 365 日）
  · 每次日线扫描完成后自动追加最新数据
  · 任何策略（手动/自动）可对任意日期区间跑日内逐步回测
  · 初始化：通过 build_store() 一次性拉取历史若干交易日

数据存储格式（每条）：
  {scan_date, scan_time, stats, stocks: [slim_fields...]}
  slim_fields: code, name, current_price, change_pct, signal_strength,
               side, value_tag, right_score, left_score, avg_amt_yi
════════════════════════════════════════════════════════
"""
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone, timedelta, date as dt_date

logger = logging.getLogger(__name__)

_CST      = timezone(timedelta(hours=8))
_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

_STORE_MAX  = 365   # 回测数据仓库最多保留天数
_BT_MAX     = 30    # 回测记录最多保留条数

# 回测数据仓库只保留这些字段（节省空间）
_SLIM_FIELDS = {
    'code', 'name', 'current_price', 'change_pct',
    'signal_strength', 'side', 'value_tag',
    'right_score', 'left_score', 'avg_amt_yi', 'market_cap_yi',
}

# ── 构建状态（全局，供 API 查询进度）────────────────────────────────────────
_build_state: dict = {
    'running':   False,
    'done':      0,
    'total':     0,
    'market':    '',
    'last_date': '',
    'error':     '',
}
_build_lock = threading.Lock()


# ── 文件路径 ──────────────────────────────────────────────────────────────────

def _store_file(market: str) -> str:
    return os.path.join(_DATA_DIR, f'backtest_store_{market}.json')

def _bt_file() -> str:
    return os.path.join(_DATA_DIR, 'backtests.json')


# ── 存储帮助 ──────────────────────────────────────────────────────────────────

def _load_store(market: str) -> list:
    try:
        with open(_store_file(market), encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def _save_store(market: str, data: list):
    os.makedirs(_DATA_DIR, exist_ok=True)
    with open(_store_file(market), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)   # 不 indent，节省空间


def _load_bt() -> dict:
    try:
        with open(_bt_file(), encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_bt(data: dict):
    os.makedirs(_DATA_DIR, exist_ok=True)
    with open(_bt_file(), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _slim(stocks: list) -> list:
    return [{k: s[k] for k in _SLIM_FIELDS if k in s} for s in stocks]


# ── 公开 API ──────────────────────────────────────────────────────────────────

def store_info(market: str = 'a_share') -> dict:
    store = _load_store(market)
    if not store:
        return {'n_days': 0, 'dates': [], 'min_date': '', 'max_date': ''}
    dates = sorted(e['scan_date'] for e in store)
    return {
        'n_days':   len(dates),
        'dates':    dates,
        'min_date': dates[0],
        'max_date': dates[-1],
    }


def available_dates(market: str = 'a_share') -> list[str]:
    """升序返回回测数据仓库中的所有交易日期"""
    return store_info(market)['dates']


def update_store(market: str, scan_data: dict, scan_date: str, scan_time: str):
    """
    日线扫描完成后调用，将最新一天数据追加到回测仓库。
    scan_data: {'right': [...], 'left': [...], 'other': [...], 'stats': {...}}
    """
    all_stocks = (
        scan_data.get('right', []) +
        scan_data.get('left',  []) +
        scan_data.get('other', [])
    )
    entry = {
        'scan_date': scan_date,
        'scan_time': scan_time,
        'stats':     scan_data.get('stats', {}),
        'stocks':    _slim(all_stocks),
    }
    store = _load_store(market)
    # 去重 + 追加
    store = [e for e in store if e['scan_date'] != scan_date]
    store.append(entry)
    # 按日期排序，截断到最大条数
    store.sort(key=lambda x: x['scan_date'])
    store = store[-_STORE_MAX:]
    _save_store(market, store)
    logger.info(f"[回测仓库] {market} 已追加 {scan_date}，共 {len(store)} 日")


def build_store_async(days: int = 90, market: str = 'a_share'):
    """在后台线程拉取最近 N 个交易日数据并写入仓库（跳过已有日期）"""
    t = threading.Thread(target=_build_store, args=(days, market), daemon=True)
    t.start()


def build_status() -> dict:
    with _build_lock:
        return dict(_build_state)


def list_backtests() -> list:
    data = _load_bt()
    return sorted(data.values(), key=lambda x: x.get('created_at', ''), reverse=True)


def get_backtest(bt_id: str) -> dict | None:
    return _load_bt().get(bt_id)


def delete_backtest(bt_id: str) -> bool:
    data = _load_bt()
    if bt_id in data:
        del data[bt_id]
        _save_bt(data)
        return True
    return False


def run_backtest(config: dict, market: str = 'a_share') -> dict:
    """
    config:
      mode          : 'auto' | 'manual'
      name          : str
      start_date    : 'YYYY-MM-DD'
      end_date      : 'YYYY-MM-DD'

      auto 模式额外字段:
        strategy_id, params

      manual 模式额外字段:
        initial_capital, max_positions, entry_signals, exit_signals,
        position_stop_loss, position_take_profit, portfolio_stop
    """
    start = config.get('start_date', '')
    end   = config.get('end_date',   '')
    mode  = config.get('mode', 'auto')

    entries = _load_range(market, start, end)
    if not entries:
        return {
            'error': f'回测仓库中无 {start} ~ {end} 的数据。\n'
                     f'请先在"自动策略"→"回测数据库"中建立历史数据，'
                     f'或在股票筛选页面对目标日期执行历史扫描。'
        }

    result = _run_auto(config, entries) if mode == 'auto' else _run_manual(config, entries)
    if 'error' in result:
        return result

    bt_id = str(uuid.uuid4())[:8]
    bt = {
        'backtest_id': bt_id,
        'status':      'done',
        'mode':        mode,
        'name':        config.get('name', f'回测-{bt_id}'),
        'strategy_id': config.get('strategy_id', ''),
        'market':      market,
        'start_date':  start,
        'end_date':    end,
        'n_days':      len(entries),
        'created_at':  datetime.now(tz=_CST).strftime('%Y-%m-%d %H:%M:%S'),
        **result,
    }
    _persist_bt(bt_id, bt)
    return bt


# ── 内部：仓库构建 ────────────────────────────────────────────────────────────

def _get_trading_days(n: int) -> list[str]:
    """返回最近 n 个真实 A 股交易日（YYYYMMDD，升序）。
    优先用 Tushare trade_cal；若接口不可用则退回到工作日估算。"""
    import data_fetcher as _df
    ts_pro = _df._ts_pro
    if ts_pro is not None:
        try:
            end   = datetime.now(tz=_CST).strftime('%Y%m%d')
            # 多取一些自然日以确保拿到足够的交易日（A股约每年244天）
            start = (datetime.now(tz=_CST) - timedelta(days=int(n * 1.8))).strftime('%Y%m%d')
            df = ts_pro.trade_cal(exchange='SSE', start_date=start,
                                  end_date=end, is_open='1')
            if df is not None and not df.empty:
                dates = sorted(df['cal_date'].tolist())
                return dates[-n:]          # 取最后 n 个
        except Exception as e:
            logger.warning(f'[回测] trade_cal 获取失败: {e}，退回工作日估算')

    # Fallback：按工作日估算（不含节假日判断）
    today  = datetime.now(tz=_CST).date()
    buffer = int(n * 1.6)              # 用足够大的自然日窗口
    result = []
    d = today
    while len(result) < n:
        if d.weekday() < 5:
            result.append(d.strftime('%Y%m%d'))
        d -= timedelta(days=1)
    result.reverse()
    return result


def _build_store(days: int, market: str):
    import data_fetcher  # 延迟导入，避免循环

    with _build_lock:
        if _build_state['running']:
            return
        _build_state.update({'running': True, 'done': 0, 'total': days,
                              'market': market, 'last_date': '', 'error': ''})

    existing = set(available_dates(market))
    candidates = _get_trading_days(days)           # days = 交易日数
    to_fetch = [d for d in candidates if
                datetime.strptime(d, '%Y%m%d').strftime('%Y-%m-%d') not in existing]

    added = 0
    with _build_lock:
        _build_state['total'] = len(to_fetch) or 1

    for i, date_ts in enumerate(to_fetch):
        date_fmt = f"{date_ts[:4]}-{date_ts[4:6]}-{date_ts[6:]}"
        with _build_lock:
            _build_state['done'] = i
            _build_state['last_date'] = date_fmt

        try:
            stocks = data_fetcher.get_a_share_stocks(
                progress_cb=lambda *_: None,
                full_scan=False,
                date=date_ts,
            )
            if not stocks:
                logger.info(f"[回测仓库构建] {date_fmt} 无数据，跳过")
                continue

            all_s  = stocks
            right  = [s for s in all_s if s.get('side') == '右侧']
            left   = [s for s in all_s if s.get('side') == '左侧']
            other  = [s for s in all_s if s.get('side') == '观望']
            data   = {'right': right, 'left': left, 'other': other,
                      'stats': {'total': len(all_s), 'right_count': len(right),
                                'left_count': len(left), 'other_count': len(other)}}
            scan_time = date_fmt + ' 15:00:00'
            update_store(market, data, date_fmt, scan_time)
            added += 1

        except Exception as e:
            logger.warning(f"[回测仓库构建] {date_fmt} 失败: {e}")

    with _build_lock:
        _build_state.update({
            'running': False,
            'done':    len(to_fetch),
            'total':   len(to_fetch) or 1,
        })
    logger.info(f"[回测仓库构建] 完成，新增 {added} 天，市场 {market}")


# ── 内部：回测运行 ────────────────────────────────────────────────────────────

def _load_range(market: str, start: str, end: str) -> list:
    store = _load_store(market)
    return [e for e in store if start <= e['scan_date'] <= end]


def _calc_metrics(nav_history: list, trades: list, initial_capital: float) -> dict:
    if not nav_history:
        return {}
    navs   = [h['nav'] for h in nav_history]
    n_days = len(navs)
    final  = navs[-1]

    # 最大回撤
    peak, max_dd = 1.0, 0.0
    for v in navs:
        if v > peak:
            peak = v
        dd = (v - peak) / peak * 100
        if dd < max_dd:
            max_dd = dd

    # 胜率
    sells    = [t for t in trades if t.get('type') == 'sell']
    wins     = [t for t in sells  if (t.get('pnl_pct') or 0) > 0]
    win_rate = round(len(wins) / len(sells) * 100, 1) if sells else 0.0
    avg_pnl  = round(sum(t.get('pnl_pct', 0) for t in sells) / len(sells), 2) if sells else 0.0

    # 年化
    years   = max(n_days / 250, 1 / 250)
    ann_ret = round(((final ** (1 / years)) - 1) * 100, 2)

    return {
        'total_return_pct':      round((final - 1.0) * 100, 2),
        'annualized_return_pct': ann_ret,
        'max_drawdown_pct':      round(max_dd, 2),
        'win_rate':              win_rate,
        'avg_trade_pnl':         avg_pnl,
        'total_sell_trades':     len(sells),
        'total_buy_trades':      len([t for t in trades if t.get('type') == 'buy']),
        'trading_days':          n_days,
        'final_nav':             round(final, 4),
    }


def _run_auto(config: dict, entries: list) -> dict:
    from auto_strategy.engine import _REGISTRY

    strategy_id = config.get('strategy_id', 'kelly_signal')
    cls = _REGISTRY.get(strategy_id)
    if cls is None:
        return {'error': f'未找到策略类型: {strategy_id}'}

    params          = config.get('params', {})
    initial_capital = float(params.get('initial_capital',
                            cls.DEFAULT_PARAMS.get('initial_capital', 1_000_000)))

    fresh_state = {
        'name': config.get('name', '回测'), 'status': 'active',
        'capital': initial_capital, 'positions': {}, 'trades': [],
        'nav_history': [], 'prev_signal_map': {}, 'action_log': [],
        'created_at': '', 'last_run': None,
    }
    inst = cls(instance_id='__backtest__',
               params={**cls.DEFAULT_PARAMS, **params},
               state=fresh_state)

    for entry in entries:
        stocks    = entry.get('stocks', [])
        scan_time = entry.get('scan_time', entry['scan_date'] + ' 15:00:00')
        try:
            inst.on_scan(stocks, scan_time, 'candidates')
        except Exception as e:
            logger.warning(f'[自动回测] {entry["scan_date"]} 失败: {e}')

    trades      = inst.state.get('trades', [])
    nav_history = inst.state.get('nav_history', [])
    metrics     = _calc_metrics(nav_history, trades, initial_capital)
    return {
        'nav_history':   nav_history,
        'trades':        trades[-500:],
        'positions':     inst.state.get('positions', {}),
        'final_capital': round(inst.state.get('capital', 0), 2),
        'final_status':  inst.state.get('status', 'active'),
        'metrics':       metrics,
    }


def _run_manual(config: dict, entries: list) -> dict:
    initial_capital  = float(config.get('initial_capital', 1_000_000))
    max_pos          = int(config.get('max_positions', 10))
    entry_signals    = config.get('entry_signals', ['⚡强烈买入'])
    exit_signals     = config.get('exit_signals',  ['🔴强烈卖出'])
    stop_loss_pct    = float(config.get('position_stop_loss', -10.0))
    take_profit_pct  = float(config.get('position_take_profit', 0.0))
    portfolio_stop   = float(config.get('portfolio_stop', -20.0))
    buy_value_tag    = '💎价值空间' in entry_signals

    capital, positions, trades, nav_hist = initial_capital, {}, [], []

    for entry in entries:
        stocks    = entry.get('stocks', [])
        scan_date = entry['scan_date']
        stock_map = {s['code']: s for s in stocks}

        # 出场
        for code in list(positions.keys()):
            pos   = positions[code]
            stock = stock_map.get(code)
            if stock is None:
                continue
            price   = float(stock.get('current_price') or pos['entry_price'])
            pnl_pct = (price - pos['entry_price']) / pos['entry_price'] * 100
            reason  = None
            if pnl_pct <= stop_loss_pct:
                reason = f'止损({pnl_pct:.1f}%)'
            elif take_profit_pct > 0 and pnl_pct >= take_profit_pct:
                reason = f'止盈({pnl_pct:.1f}%)'
            elif stock.get('signal_strength') in exit_signals:
                reason = f'离场({stock.get("signal_strength","")})'
            if reason:
                proceeds = pos['shares'] * price * (1 - 0.0015)
                capital  = round(capital + proceeds, 2)
                trades.append({'date': scan_date, 'type': 'sell',
                                'code': code, 'name': pos.get('name', code),
                                'price': round(price, 3), 'shares': pos['shares'],
                                'proceeds': round(proceeds, 2),
                                'pnl_pct': round(pnl_pct, 2), 'reason': reason})
                del positions[code]

        # 组合止损
        port_val = capital + sum(
            pos['shares'] * float(stock_map.get(c, {}).get('current_price') or pos['entry_price'])
            for c, pos in positions.items()
        )
        if (port_val - initial_capital) / initial_capital * 100 <= portfolio_stop:
            for code, pos in list(positions.items()):
                price    = float(stock_map.get(code, {}).get('current_price') or pos['entry_price'])
                proceeds = pos['shares'] * price * (1 - 0.0015)
                capital  = round(capital + proceeds, 2)
                trades.append({'date': scan_date, 'type': 'sell', 'code': code,
                                'name': pos.get('name', code), 'price': round(price, 3),
                                'shares': pos['shares'],
                                'pnl_pct': round((price - pos['entry_price']) / pos['entry_price'] * 100, 2),
                                'reason': '组合止损'})
            positions = {}

        # 入场
        slot_value = initial_capital / max_pos
        candidates = sorted(
            [s for s in stocks
             if s['code'] not in positions
             and (s.get('signal_strength') in entry_signals
                  or (buy_value_tag and '价值空间' in (s.get('value_tag') or '')))],
            key=lambda x: max(x.get('right_score', 0), x.get('left_score', 0)),
            reverse=True,
        )
        for s in candidates:
            if len(positions) >= max_pos:
                break
            price  = float(s.get('current_price') or 0)
            if price <= 0:
                continue
            shares = int(slot_value / price / 100) * 100
            if shares <= 0:
                continue
            cost = shares * price * 1.0005
            if cost > capital:
                continue
            capital = round(capital - cost, 2)
            positions[s['code']] = {
                'name': s.get('name', s['code']),
                'entry_price': price, 'shares': shares,
                'cost': round(cost, 2), 'entry_date': scan_date,
            }
            trades.append({'date': scan_date, 'type': 'buy', 'code': s['code'],
                           'name': s.get('name', s['code']), 'price': round(price, 3),
                           'shares': shares, 'cost': round(cost, 2)})

        # NAV
        port_val = capital + sum(
            pos['shares'] * float(stock_map.get(c, {}).get('current_price') or pos['entry_price'])
            for c, pos in positions.items()
        )
        nav_hist.append({'date': scan_date, 'nav': round(port_val / initial_capital, 4),
                         'total_value': round(port_val, 2), 'n_positions': len(positions)})

    metrics = _calc_metrics(nav_hist, trades, initial_capital)
    return {
        'nav_history':   nav_hist,
        'trades':        trades[-500:],
        'positions':     positions,
        'final_capital': round(capital, 2),
        'final_status':  'active',
        'metrics':       metrics,
    }


def _persist_bt(bt_id: str, bt: dict):
    data = _load_bt()
    data[bt_id] = bt
    if len(data) > _BT_MAX:
        oldest = sorted(data.keys(), key=lambda k: data[k].get('created_at', ''))
        for k in oldest[:len(data) - _BT_MAX]:
            del data[k]
    _save_bt(data)
