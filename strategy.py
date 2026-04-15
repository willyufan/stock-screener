"""
虚拟交易策略引擎
存储在 data/strategies.json
"""
import json
import os
import uuid
from datetime import datetime

_STRAT_FILE = os.path.join(os.path.dirname(__file__), 'data', 'strategies.json')


def _load_all():
    try:
        with open(_STRAT_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_all(data):
    os.makedirs(os.path.dirname(_STRAT_FILE), exist_ok=True)
    with open(_STRAT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def list_strategies():
    data = _load_all()
    return list(data.values())


def create_strategy(params: dict) -> dict:
    data = _load_all()
    sid = str(uuid.uuid4())[:8]
    initial_capital = float(params.get('initial_capital', 1_000_000))
    strategy = {
        'id': sid,
        'name': params.get('name', f'策略-{sid}'),
        'type': params.get('type', 'custom'),
        'market': params.get('market', 'a_share'),
        'initial_capital': initial_capital,
        'max_positions': int(params.get('max_positions', 10)),
        'entry_signals': params.get('entry_signals', ['⚡强烈买入', '✅买入']),
        'exit_signals': params.get('exit_signals', ['🔴强烈卖出', '⚠️卖出观察']),
        'position_stop_loss':   float(params.get('position_stop_loss', -10.0)),
        'position_take_profit': float(params.get('position_take_profit', 0.0)),  # 0=不启用
        'portfolio_stop':       float(params.get('portfolio_stop', -20.0)),
        # State
        'capital': initial_capital,
        'positions': {},
        'trades': [],
        'nav_history': [],
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'last_run': None,
    }
    data[sid] = strategy
    _save_all(data)
    return strategy


def delete_strategy(sid: str) -> bool:
    data = _load_all()
    if sid in data:
        del data[sid]
        _save_all(data)
        return True
    return False


def _get_strategy(sid: str):
    data = _load_all()
    return data.get(sid), data


def run_on_scan(sid: str, all_stocks: list, scan_date: str) -> dict:
    """
    Apply strategy to a list of stocks from scan.
    Process:
    1) Check stop losses on positions
    2) Check exit signals on positions
    3) Buy entry candidates
    4) Compute NAV
    5) Check -20% portfolio liquidation
    6) Save
    """
    strat, all_data = _get_strategy(sid)
    if not strat:
        return {'error': 'strategy not found'}

    if strat['status'] == 'liquidated':
        return {'error': 'strategy has been liquidated'}

    stock_map = {s['code']: s for s in all_stocks}
    positions = strat.get('positions', {})
    capital = strat['capital']
    trades = strat.get('trades', [])
    initial_capital = strat['initial_capital']

    # 1) Check stop losses and exit signals on current positions
    codes_to_sell = []
    for code, pos in positions.items():
        current_stock = stock_map.get(code)
        if current_stock is None:
            continue
        current_price = current_stock.get('current_price', pos['entry_price'])
        pnl_pct = (current_price - pos['entry_price']) / pos['entry_price'] * 100

        sell_reason = None
        tp = strat.get('position_take_profit', 0.0)
        # Stop loss check
        if pnl_pct <= strat['position_stop_loss']:
            sell_reason = f'止损({pnl_pct:.1f}%)'
        # Take profit check (only when > 0)
        elif tp > 0 and pnl_pct >= tp:
            sell_reason = f'止盈({pnl_pct:.1f}%)'
        # Exit signal check
        elif current_stock.get('signal_strength') in strat.get('exit_signals', []):
            sell_reason = f'离场信号({current_stock.get("signal_strength","")})'

        if sell_reason:
            codes_to_sell.append((code, current_price, sell_reason))

    # Execute sells
    for code, sell_price, reason in codes_to_sell:
        pos = positions[code]
        proceeds = pos['shares'] * sell_price * 0.999  # 0.1% fee
        capital += proceeds
        pnl_pct = (sell_price - pos['entry_price']) / pos['entry_price'] * 100
        trade = {
            'date': scan_date,
            'action': 'sell',
            'code': code,
            'name': pos['name'],
            'price': sell_price,
            'shares': pos['shares'],
            'proceeds': round(proceeds, 2),
            'pnl_pct': round(pnl_pct, 2),
            'reason': reason,
        }
        trades.append(trade)
        del positions[code]

    # 2) Buy entry candidates
    # Equal weight allocation: capital * 0.9 / max_positions
    max_pos = strat['max_positions']
    slot_value = initial_capital * 0.9 / max_pos
    is_hk = strat.get('market', 'a_share') == 'hk'

    entry_signals = strat.get('entry_signals', ['⚡强烈买入', '✅买入'])
    # Also allow value tag matching
    buy_value_tag = '💎价值空间' in entry_signals or '价值空间' in ' '.join(entry_signals)

    candidates = []
    for s in all_stocks:
        if s['code'] in positions:
            continue  # already held
        sig = s.get('signal_strength', '')
        val = s.get('value_tag', '') or ''
        matches_entry = sig in entry_signals
        matches_value = buy_value_tag and '价值空间' in val
        if matches_entry or matches_value:
            candidates.append(s)

    # Sort by right_score or left_score descending
    candidates.sort(key=lambda x: max(x.get('right_score', 0), x.get('left_score', 0)), reverse=True)

    for s in candidates:
        if len(positions) >= max_pos:
            break
        price = s.get('current_price', 0)
        if not price or price <= 0:
            continue

        shares_raw = slot_value / price
        if is_hk:
            shares = int(shares_raw)
        else:
            # Round down to lot of 100
            shares = int(shares_raw / 100) * 100

        if shares <= 0:
            continue

        cost = shares * price * 1.001  # 0.1% fee
        if cost > capital:
            continue

        capital -= cost
        positions[s['code']] = {
            'name': s.get('name', s['code']),
            'entry_price': price,
            'shares': shares,
            'cost': round(cost, 2),
            'entry_date': scan_date,
            'signal': s.get('signal_strength', ''),
        }
        trade = {
            'date': scan_date,
            'action': 'buy',
            'code': s['code'],
            'name': s.get('name', s['code']),
            'price': price,
            'shares': shares,
            'cost': round(cost, 2),
            'reason': f'入场信号({s.get("signal_strength","")})',
        }
        trades.append(trade)

    # 3) Compute NAV
    portfolio_value = capital
    for code, pos in positions.items():
        current_stock = stock_map.get(code)
        if current_stock:
            cur_price = current_stock.get('current_price', pos['entry_price'])
        else:
            cur_price = pos['entry_price']
        portfolio_value += pos['shares'] * cur_price

    nav = portfolio_value / initial_capital
    nav_entry = {
        'date': scan_date,
        'nav': round(nav, 4),
        'total_value': round(portfolio_value, 2),
        'n_positions': len(positions),
    }
    nav_history = strat.get('nav_history', [])
    # Update or append by date
    if nav_history and nav_history[-1]['date'] == scan_date:
        nav_history[-1] = nav_entry
    else:
        nav_history.append(nav_entry)

    # 4) Check portfolio stop loss (-20%)
    total_return_pct = (portfolio_value - initial_capital) / initial_capital * 100
    if total_return_pct <= strat['portfolio_stop']:
        # Liquidate all positions
        for code, pos in list(positions.items()):
            current_stock = stock_map.get(code)
            if current_stock:
                sell_price = current_stock.get('current_price', pos['entry_price'])
            else:
                sell_price = pos['entry_price']
            proceeds = pos['shares'] * sell_price * 0.999
            capital += proceeds
            pnl_pct = (sell_price - pos['entry_price']) / pos['entry_price'] * 100
            trades.append({
                'date': scan_date,
                'action': 'sell',
                'code': code,
                'name': pos['name'],
                'price': sell_price,
                'shares': pos['shares'],
                'proceeds': round(proceeds, 2),
                'pnl_pct': round(pnl_pct, 2),
                'reason': '组合止损清盘',
            })
        positions = {}
        strat['status'] = 'liquidated'

    # 5) Update and save
    strat['capital'] = round(capital, 2)
    strat['positions'] = positions
    strat['trades'] = trades[-200:]
    strat['nav_history'] = nav_history
    strat['last_run'] = scan_date

    # Compute summary stats
    if len(nav_history) >= 2:
        max_nav = max(h['nav'] for h in nav_history)
        current_nav = nav_history[-1]['nav']
        max_drawdown = (current_nav - max_nav) / max_nav * 100
    else:
        max_drawdown = 0.0

    strat['summary'] = {
        'total_return_pct': round(total_return_pct, 2),
        'current_nav': round(nav, 4),
        'max_drawdown_pct': round(max_drawdown, 2),
        'n_positions': len(positions),
        'total_trades': len(trades),
    }

    all_data[sid] = strat
    _save_all(all_data)
    return strat
