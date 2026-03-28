"""
自选池管理
存储在 data/watchlist.json
"""
import json, os, uuid
from datetime import datetime

_WL_FILE = os.path.join(os.path.dirname(__file__), 'data', 'watchlist.json')


def _load():
    try:
        with open(_WL_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def _save(wl):
    os.makedirs(os.path.dirname(_WL_FILE), exist_ok=True)
    with open(_WL_FILE, 'w', encoding='utf-8') as f:
        json.dump(wl, f, ensure_ascii=False, indent=2)


def get_watchlist():
    return _load()


def add_stock(code: str, name: str, market: str, notes: str = '') -> bool:
    wl = _load()
    if any(s['code'] == code for s in wl):
        return False  # already exists
    wl.append({
        'id': str(uuid.uuid4())[:8],
        'code': code,
        'name': name,
        'market': market,
        'notes': notes,
        'added_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status_history': [],
        'current_status': None,
    })
    _save(wl)
    return True


def remove_stock(code: str) -> bool:
    wl = _load()
    before = len(wl)
    wl = [s for s in wl if s['code'] != code]
    if len(wl) < before:
        _save(wl)
        return True
    return False


def update_all_statuses(stocks_list: list):
    """Update current_status and status_history from latest scan results"""
    wl = _load()
    today = datetime.now().strftime('%Y-%m-%d')
    stock_map = {s['code']: s for s in stocks_list}
    for entry in wl:
        if entry['code'] in stock_map:
            s = stock_map[entry['code']]
            status = {
                'date':            today,
                'side':            s.get('side'),
                'signal_strength': s.get('signal_strength'),
                'value_tag':       s.get('value_tag'),
                'change_pct':      s.get('change_pct'),
                'price':           s.get('current_price'),
                'right_score':     s.get('right_score', 0),
                'left_score':      s.get('left_score', 0),
                'ma5':             s.get('ma5'),
                'ma10':            s.get('ma10'),
                'ma20':            s.get('ma20'),
                'rsi14':           s.get('rsi14'),
                'price_position':  s.get('price_position'),
                'signals':         s.get('signals', []),
            }
            hist = entry.get('status_history', [])
            if not hist or hist[-1].get('date') != today:
                hist.append(status)
                entry['status_history'] = hist[-60:]
            entry['current_status'] = status
    _save(wl)
    return wl
