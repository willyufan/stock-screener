"""
凯利转折策略
════════════════════════════════════════════════════════
买入：信号从 左侧/观望 → ⚡强烈买入（当前扫描 vs 上次记录的信号）
仓位：半凯利 = 12.5% / 只（p=0.55, b=1.5 → f*=0.25 → half=0.125）
止损：-8%（T+1，当日买入不触发）
止盈①：+10% → 卖半仓，标记 half_profit
止盈②：回落至保本（0%）→ 清剩余（仅 half_profit 后激活）
离场：信号从右侧 → 左侧/观望 → 全清剩余（T+1）
手续费：买 0.05%，卖 0.15%（含印花税）
最大持仓：6 只
════════════════════════════════════════════════════════
"""
import logging
from auto_strategy.base import AutoStrategyBase
from auto_strategy.engine import register

logger = logging.getLogger(__name__)

# 凯利计算：p=0.55, b=1.5
# f* = (b*p - (1-p)) / b = (0.825 - 0.45) / 1.5 = 0.25
# 半凯利 = 0.125
KELLY_FRACTION = 0.125

BUY_SIDE_PREV  = {'左侧', '观望'}       # 这些 side 转为强烈买入才触发
BUY_SIGNAL     = '⚡强烈买入'
RIGHT_SIDE     = '右侧'
WEAK_SIDES     = {'左侧', '观望'}        # 持仓变为此 side 触发离场


@register
class KellySignalStrategy(AutoStrategyBase):
    STRATEGY_ID   = 'kelly_signal'
    STRATEGY_NAME = '凯利转折'
    DEFAULT_PARAMS = {
        'initial_capital':    1_000_000,
        'max_positions':      6,
        'kelly_fraction':     KELLY_FRACTION,   # 每只仓位占总资金比例
        'stop_loss_pct':      -8.0,             # -8% 止损
        'take_profit1_pct':   10.0,             # +10% 卖半仓
        'take_profit2_pct':   0.0,              # 回落至 0% 清剩余（保本止损）
        'buy_fee':            0.0005,           # 0.05%
        'sell_fee':           0.0015,           # 0.15%（含印花税）
    }

    # ── 主入口 ───────────────────────────────────────────────────

    def on_scan(self, stocks: list, scan_time: str, scan_type: str) -> dict:
        if self.status == 'liquidated':
            return {'actions': [], 'state': self.state}

        today       = self.today(scan_time)
        stock_map   = {s['code']: s for s in stocks}
        actions     = []

        # ── 1. 处理现有持仓（止损 / 止盈 / 离场）────────────────
        exits = self._process_exits(stock_map, scan_time, today, actions)
        for code in exits:
            del self.state['positions'][code]

        # ── 2. 寻找买入信号（仅 candidates 扫描触发，positions 扫描不买新仓）
        if scan_type == 'candidates':
            self._process_entries(stocks, stock_map, scan_time, today, actions)

        # ── 3. 更新 prev_signal_map（记录本次所有股票的信号）──────
        for s in stocks:
            self.state['prev_signal_map'][s['code']] = {
                'signal_strength': s.get('signal_strength', ''),
                'side':            s.get('side', ''),
            }

        # ── 4. 计算 NAV ─────────────────────────────────────────
        self._update_nav(stock_map, scan_time, today)

        # ── 5. 检查组合总止损（回撤 > 20% 清盘）────────────────
        self._check_portfolio_stop(stock_map, scan_time, actions)

        # ── 6. 记录 action log ──────────────────────────────────
        if actions:
            log = self.state.setdefault('action_log', [])
            for a in actions:
                log.append({**a, 'scan_time': scan_time})
            self.state['action_log'] = log[-200:]   # 保留最近 200 条

        self.state['last_run'] = scan_time
        return {'actions': actions, 'state': self.state}

    # ── 出场逻辑 ─────────────────────────────────────────────────

    def _process_exits(self, stock_map, scan_time, today, actions) -> list:
        p      = self.params
        sl_pct = p['stop_loss_pct']       # -8.0
        tp1    = p['take_profit1_pct']    # 10.0
        tp2    = p['take_profit2_pct']    #  0.0
        codes_to_exit = []

        for code, pos in self.state['positions'].items():
            # T+1：当日买入跳过所有出场检查
            if self.is_trading_day_buy(pos, scan_time):
                continue

            stock = stock_map.get(code)
            if stock is None:
                continue

            cur_price  = float(stock.get('current_price', pos['entry_price']))
            entry      = float(pos['entry_price'])
            pnl_pct    = (cur_price - entry) / entry * 100
            half_taken = pos.get('half_profit_taken', False)
            shares_rem = int(pos.get('shares_remaining', pos['shares']))

            reason = None
            sell_all = True
            sell_shares = shares_rem

            # ① 止损 -8%（优先级最高）
            if pnl_pct <= sl_pct:
                reason = f'止损 {pnl_pct:.1f}%'

            # ② 止盈① +10% → 卖半仓（仅在未触发过时）
            elif not half_taken and pnl_pct >= tp1:
                half = max(100, (shares_rem // 2 // 100) * 100)   # 向下取整到100股
                if half >= 100:
                    sell_shares = half
                    sell_all    = False
                    reason      = f'止盈① {pnl_pct:.1f}%，卖出 {half} 股'

            # ③ 保本止损（half_profit 后，回落至 tp2=0%）
            elif half_taken and pnl_pct <= tp2:
                reason = f'保本止损 {pnl_pct:.1f}%（止盈①后保本）'

            # ④ 信号离场：右侧 → 左侧/观望
            elif stock.get('side', '') in WEAK_SIDES:
                reason = f'信号离场（{stock.get("side")}）'

            if reason is None:
                continue

            self._execute_sell(
                code=code, pos=pos, price=cur_price,
                shares=sell_shares, reason=reason,
                scan_time=scan_time, actions=actions,
            )

            if sell_all:
                codes_to_exit.append(code)
            else:
                # 只卖了一半，更新持仓
                pos['shares_remaining'] = shares_rem - sell_shares
                pos['half_profit_taken'] = True
                pos['half_profit_price'] = round(cur_price, 3)
                pos['half_profit_date']  = today

        return codes_to_exit

    # ── 买入逻辑 ─────────────────────────────────────────────────

    def _process_entries(self, stocks, stock_map, scan_time, today, actions):
        p            = self.params
        max_pos      = int(p['max_positions'])
        kelly        = float(p['kelly_fraction'])
        initial_cap  = float(p['initial_capital'])
        buy_fee      = float(p['buy_fee'])
        prev_map     = self.state.get('prev_signal_map', {})

        if len(self.state['positions']) >= max_pos:
            return

        # 找转折信号：上次不是强烈买入 & 当前是强烈买入 & 之前 side 是左侧/观望
        candidates = []
        for s in stocks:
            code = s.get('code', '')
            if code in self.state['positions']:
                continue
            if s.get('signal_strength') != BUY_SIGNAL:
                continue
            prev = prev_map.get(code, {})
            prev_side = prev.get('side', '')
            prev_sig  = prev.get('signal_strength', '')
            # 首次扫描（prev 为空）不买入，等待下次确认转折
            if not prev:
                continue
            if prev_side not in BUY_SIDE_PREV and prev_sig == BUY_SIGNAL:
                continue   # 之前已经是强烈买入，不是新转折
            # 不追涨停（涨幅 >= 9.9%）
            if float(s.get('change_pct', 0)) >= 9.9:
                continue
            candidates.append(s)

        # 按 right_score 排序，依次买入
        candidates.sort(key=lambda x: x.get('right_score', 0), reverse=True)

        for s in candidates:
            if len(self.state['positions']) >= max_pos:
                break

            price = float(s.get('current_price', 0))
            if price <= 0:
                continue

            slot_value = initial_cap * kelly
            shares_raw = slot_value / price
            shares     = int(shares_raw / 100) * 100   # A股：整百股
            if shares <= 0:
                continue

            cost = shares * price * (1 + buy_fee)
            if cost > self.state['capital']:
                continue

            self.state['capital'] -= cost
            self.state['positions'][s['code']] = {
                'name':               s.get('name', s['code']),
                'entry_price':        round(price, 3),
                'shares':             shares,
                'shares_remaining':   shares,
                'cost':               round(cost, 2),
                'entry_date':         today,
                'half_profit_taken':  False,
                'half_profit_price':  None,
                'half_profit_date':   None,
                'stop_loss_price':    round(price * (1 + self.params['stop_loss_pct'] / 100), 3),
            }
            action = {
                'type':    'buy',
                'code':    s['code'],
                'name':    s.get('name', s['code']),
                'price':   round(price, 3),
                'shares':  shares,
                'cost':    round(cost, 2),
                'reason':  f'转折信号 {s.get("side")} → {BUY_SIGNAL}',
            }
            self.state['trades'].append({**action, 'date': today})
            self.state['trades'] = self.state['trades'][-500:]
            actions.append(action)
            logger.info(f"[凯利转折] 买入 {s['code']} {s.get('name')} "
                        f"×{shares}股 @{price:.3f}，花费 {cost:.0f}")

    # ── 工具方法 ─────────────────────────────────────────────────

    def _execute_sell(self, code, pos, price, shares, reason, scan_time, actions):
        sell_fee  = self.params['sell_fee']
        proceeds  = shares * price * (1 - sell_fee)
        entry     = pos['entry_price']
        pnl_pct   = (price - entry) / entry * 100

        self.state['capital'] = round(self.state['capital'] + proceeds, 2)
        trade = {
            'date':     self.today(scan_time),
            'type':     'sell',
            'code':     code,
            'name':     pos.get('name', code),
            'price':    round(price, 3),
            'shares':   shares,
            'proceeds': round(proceeds, 2),
            'pnl_pct':  round(pnl_pct, 2),
            'reason':   reason,
        }
        self.state['trades'].append(trade)
        self.state['trades'] = self.state['trades'][-500:]
        actions.append({'type': 'sell', **trade})
        logger.info(f"[凯利转折] 卖出 {code} ×{shares}股 @{price:.3f}，"
                    f"收益 {pnl_pct:.1f}%，原因: {reason}")

    def _update_nav(self, stock_map, scan_time, today):
        initial    = self.params['initial_capital']
        port_value = self.state['capital']
        for code, pos in self.state['positions'].items():
            stock = stock_map.get(code)
            cur_p = float(stock['current_price']) if stock else float(pos['entry_price'])
            port_value += int(pos.get('shares_remaining', pos['shares'])) * cur_p

        nav = port_value / initial
        entry = {
            'date':        scan_time[:16],
            'nav':         round(nav, 4),
            'total_value': round(port_value, 2),
            'n_positions': len(self.state['positions']),
        }
        hist = self.state.setdefault('nav_history', [])
        # 同一分钟更新，不同分钟追加
        if hist and hist[-1]['date'][:16] == scan_time[:16]:
            hist[-1] = entry
        else:
            hist.append(entry)
        self.state['nav_history'] = hist[-2000:]

    def _check_portfolio_stop(self, stock_map, scan_time, actions):
        initial    = self.params['initial_capital']
        port_value = self.state['capital']
        for code, pos in self.state['positions'].items():
            stock = stock_map.get(code)
            cur_p = float(stock['current_price']) if stock else float(pos['entry_price'])
            port_value += int(pos.get('shares_remaining', pos['shares'])) * cur_p

        drawdown_pct = (port_value - initial) / initial * 100
        if drawdown_pct > -20.0:
            return

        logger.warning(f"[凯利转折] 组合回撤 {drawdown_pct:.1f}%，触发清盘！")
        today = self.today(scan_time)
        for code, pos in list(self.state['positions'].items()):
            stock     = stock_map.get(code)
            cur_p     = float(stock['current_price']) if stock else float(pos['entry_price'])
            shares_rem = int(pos.get('shares_remaining', pos['shares']))
            self._execute_sell(
                code=code, pos=pos, price=cur_p,
                shares=shares_rem, reason='组合回撤 -20% 清盘',
                scan_time=scan_time, actions=actions,
            )
        self.state['positions'] = {}
        self.state['status']    = 'liquidated'
        actions.append({'type': 'liquidated', 'reason': f'组合回撤 {drawdown_pct:.1f}%'})
