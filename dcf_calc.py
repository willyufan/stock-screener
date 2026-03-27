"""
DCF（未来现金流折现）估值计算模块
"""
import json
import os
import uuid
from datetime import datetime

_DCF_FILE = os.path.join(os.path.dirname(__file__), 'data', 'dcf_history.json')


def _load_history() -> list:
    try:
        with open(_DCF_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def _save_history(items: list):
    os.makedirs(os.path.dirname(_DCF_FILE), exist_ok=True)
    with open(_DCF_FILE, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def calculate_dcf(params: dict) -> dict:
    """
    params keys:
      fcf0      - 基准FCF（亿元或百万HKD）
      g1,g2,g3  - 第1-3年、4-7年、8-10年增速
      gterm     - 永续增速
      wacc      - 折现率
      net_cash  - 净现金（同单位）
      shares    - 总股本（亿股或百万股）
    返回:
      dcf_table, terminal_value, total_pv, equity_value, intrinsic_per_share
    """
    fcf0   = float(params['fcf0'])
    g1     = float(params['g1'])
    g2     = float(params['g2'])
    g3     = float(params['g3'])
    gterm  = float(params['gterm'])
    wacc   = float(params['wacc'])
    net_cash = float(params.get('net_cash', 0))
    shares   = float(params['shares'])

    table = []
    fcf_prev = fcf0
    total_pv = 0.0

    for yr in range(1, 11):
        if yr <= 3:
            g = g1
        elif yr <= 7:
            g = g2
        else:
            g = g3
        fcf = fcf_prev * (1 + g)
        pv  = fcf / (1 + wacc) ** yr
        total_pv += pv
        table.append({
            'year':    yr,
            'growth':  round(g * 100, 1),
            'fcf':     round(fcf, 2),
            'pv':      round(pv, 2),
        })
        fcf_prev = fcf

    # Terminal value (Gordon Growth)
    if wacc <= gterm:
        terminal_value = fcf_prev * 15  # fallback P/E of 15
    else:
        terminal_value = fcf_prev * (1 + gterm) / (wacc - gterm)
    terminal_pv = terminal_value / (1 + wacc) ** 10
    total_pv += terminal_pv

    equity_value = total_pv + net_cash
    intrinsic_per_share = equity_value / shares if shares > 0 else 0

    return {
        'dcf_table':            table,
        'terminal_value':       round(terminal_value, 2),
        'terminal_pv':          round(terminal_pv, 2),
        'total_pv':             round(total_pv, 2),
        'equity_value':         round(equity_value, 2),
        'intrinsic_per_share':  round(intrinsic_per_share, 4),
    }


def list_analyses() -> list:
    return _load_history()


def save_analysis(data: dict) -> str:
    items = _load_history()
    aid = str(uuid.uuid4())[:8]
    items.insert(0, {
        'id':         aid,
        'code':       data.get('code', ''),
        'name':       data.get('name', ''),
        'market':     data.get('market', 'A股'),
        'currency':   data.get('currency', '¥'),
        'conv':       data.get('conv', 1.0),
        'params':     data.get('params', {}),
        'result':     data.get('result', {}),
        'notes':      data.get('notes', ''),
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
    })
    _save_history(items)
    return aid


def delete_analysis(aid: str) -> bool:
    items = _load_history()
    new_items = [x for x in items if x.get('id') != aid]
    if len(new_items) == len(items):
        return False
    _save_history(new_items)
    return True
