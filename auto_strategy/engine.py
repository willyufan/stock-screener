"""
自动策略引擎
- 管理多个策略实例（增删查）
- 每次扫描时驱动所有 active 实例运行
- 持久化到 data/auto_strategies.json
"""
import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta

from auto_strategy.base import AutoStrategyBase

logger = logging.getLogger(__name__)

_DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'auto_strategies.json')
_DATA_FILE = os.path.normpath(_DATA_FILE)

# 注册表：strategy_id → class
_REGISTRY: dict[str, type[AutoStrategyBase]] = {}


def register(cls: type[AutoStrategyBase]):
    """装饰器：将策略类注册到引擎"""
    _REGISTRY[cls.STRATEGY_ID] = cls
    return cls


def available_strategies() -> list[dict]:
    """返回所有已注册策略的元信息"""
    return [
        {'strategy_id': cls.STRATEGY_ID,
         'strategy_name': cls.STRATEGY_NAME,
         'default_params': cls.DEFAULT_PARAMS}
        for cls in _REGISTRY.values()
    ]


# ── 持久化 ───────────────────────────────────────────────────────

def _load_all() -> dict:
    try:
        with open(_DATA_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_all(data: dict):
    os.makedirs(os.path.dirname(_DATA_FILE), exist_ok=True)
    with open(_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── 实例工厂 ─────────────────────────────────────────────────────

def _build_instance(raw: dict) -> AutoStrategyBase | None:
    sid = raw.get('strategy_id', '')
    cls = _REGISTRY.get(sid)
    if cls is None:
        logger.warning(f"策略类 {sid} 未注册，跳过")
        return None
    return cls(
        instance_id=raw['instance_id'],
        params=raw.get('params', {}),
        state=raw.get('state', {}),
    )


# ── 公开 API ─────────────────────────────────────────────────────

def list_instances() -> list[dict]:
    data = _load_all()
    result = []
    for raw in data.values():
        inst = _build_instance(raw)
        if inst:
            result.append(inst.to_dict())
    return result


def get_instance(instance_id: str) -> dict | None:
    data = _load_all()
    raw = data.get(instance_id)
    if not raw:
        return None
    inst = _build_instance(raw)
    return inst.to_dict() if inst else None


def create_instance(strategy_id: str, name: str, params: dict) -> dict | str:
    cls = _REGISTRY.get(strategy_id)
    if cls is None:
        return f'未知策略: {strategy_id}'

    instance_id = str(uuid.uuid4())[:8]
    initial_capital = float(params.get('initial_capital',
                            cls.DEFAULT_PARAMS.get('initial_capital', 1_000_000)))
    state = {
        'name':            name or f'{cls.STRATEGY_NAME}-{instance_id}',
        'status':          'active',
        'capital':         initial_capital,
        'positions':       {},
        'trades':          [],
        'nav_history':     [],
        'prev_signal_map': {},   # code → previous signal_strength
        'action_log':      [],   # 最近扫描事件记录
        'created_at':      datetime.now(tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        'last_run':        None,
    }
    raw = {
        'instance_id':   instance_id,
        'strategy_id':   strategy_id,
        'strategy_name': cls.STRATEGY_NAME,
        'params':        {**cls.DEFAULT_PARAMS, **params},
        'state':         state,
    }
    data = _load_all()
    data[instance_id] = raw
    _save_all(data)
    inst = _build_instance(raw)
    return inst.to_dict()


def delete_instance(instance_id: str) -> bool:
    data = _load_all()
    if instance_id in data:
        del data[instance_id]
        _save_all(data)
        return True
    return False


def reset_instance(instance_id: str) -> dict | None:
    data = _load_all()
    raw = data.get(instance_id)
    if not raw:
        return None
    cls = _REGISTRY.get(raw.get('strategy_id', ''))
    if not cls:
        return None
    initial_capital = float(raw['params'].get('initial_capital',
                            cls.DEFAULT_PARAMS.get('initial_capital', 1_000_000)))
    raw['state'] = {
        **raw['state'],
        'status':          'active',
        'capital':         initial_capital,
        'positions':       {},
        'trades':          [],
        'nav_history':     [],
        'prev_signal_map': {},
        'action_log':      [],
        'last_run':        None,
    }
    data[instance_id] = raw
    _save_all(data)
    inst = _build_instance(raw)
    return inst.to_dict() if inst else None


def run_scan(stocks: list[dict], scan_time: str, scan_type: str) -> list[dict]:
    """
    将扫描结果喂给所有 active 实例运行。
    scan_type: 'positions' | 'candidates'
    返回每个实例的 to_dict()（已更新）
    """
    data = _load_all()
    results = []
    changed = False

    for instance_id, raw in data.items():
        if raw.get('state', {}).get('status') == 'liquidated':
            continue
        inst = _build_instance(raw)
        if inst is None:
            continue
        try:
            inst.on_scan(stocks, scan_time, scan_type)
            # 回写更新后的 state
            raw['state'] = inst.state
            results.append(inst.to_dict())
            changed = True
        except Exception as e:
            logger.exception(f"策略实例 {instance_id} 运行失败: {e}")

    if changed:
        _save_all(data)
    return results
