"""
自动策略抽象基类
所有策略必须继承此类并实现 on_scan()
"""
from abc import ABC, abstractmethod
from typing import Any


class AutoStrategyBase(ABC):
    # 子类必须定义
    STRATEGY_ID: str = ''       # 唯一标识，如 'kelly_signal'
    STRATEGY_NAME: str = ''     # 展示名，如 '凯利转折'
    DEFAULT_PARAMS: dict = {}   # 默认参数

    def __init__(self, instance_id: str, params: dict, state: dict):
        self.instance_id = instance_id
        self.params = {**self.DEFAULT_PARAMS, **params}
        self.state = state

    # ── 子类必须实现 ─────────────────────────────────────────────

    @abstractmethod
    def on_scan(self, stocks: list[dict], scan_time: str,
                scan_type: str) -> dict:
        """
        每次扫描时调用。
        stocks    : 本次扫描到的股票列表（已含 signal_strength / side 等字段）
        scan_time : 'YYYY-MM-DD HH:MM:SS'
        scan_type : 'positions'（5分钟，仅持仓+自选）或 'candidates'（15分钟，候选池）
        返回      : {'actions': [...], 'state': self.state}
        """

    # ── 通用工具（子类可直接用）──────────────────────────────────

    @property
    def capital(self) -> float:
        return self.state.get('capital', 0.0)

    @property
    def positions(self) -> dict:
        return self.state.get('positions', {})

    @property
    def trades(self) -> list:
        return self.state.get('trades', [])

    @property
    def nav_history(self) -> list:
        return self.state.get('nav_history', [])

    @property
    def status(self) -> str:
        return self.state.get('status', 'active')

    def today(self, scan_time: str) -> str:
        return scan_time[:10]

    def is_trading_day_buy(self, pos: dict, scan_time: str) -> bool:
        """T+1：判断该持仓是否是今天买入（今天买入不能今天卖）"""
        return pos.get('entry_date', '') == self.today(scan_time)

    def get_summary(self) -> dict:
        initial = self.params.get('initial_capital', 1_000_000)
        nav_hist = self.nav_history
        if not nav_hist:
            return {
                'total_return_pct': 0.0,
                'current_nav': 1.0,
                'max_drawdown_pct': 0.0,
                'n_positions': 0,
                'total_trades': 0,
            }
        current_nav = nav_hist[-1]['nav']
        max_nav = max(h['nav'] for h in nav_hist)
        max_dd = (current_nav - max_nav) / max_nav * 100 if max_nav > 0 else 0.0
        total_value = nav_hist[-1].get('total_value', initial * current_nav)
        return {
            'total_return_pct': round((total_value - initial) / initial * 100, 2),
            'current_nav': round(current_nav, 4),
            'max_drawdown_pct': round(max_dd, 2),
            'n_positions': len(self.positions),
            'total_trades': len(self.trades),
        }

    def to_dict(self) -> dict:
        """序列化为可 JSON 存储的 dict"""
        return {
            'instance_id':   self.instance_id,
            'strategy_id':   self.STRATEGY_ID,
            'strategy_name': self.STRATEGY_NAME,
            'name':          self.state.get('name', self.STRATEGY_NAME),
            'params':        self.params,
            'state':         self.state,
            'summary':       self.get_summary(),
        }
