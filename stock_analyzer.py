"""
左侧/右侧交易信号分析
右侧：趋势确认向上，追涨信号
左侧：超跌触底，抄底信号
"""
import pandas as pd
import numpy as np


def calc_rsi(prices: pd.Series, period: int = 14) -> float:
    delta = prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean().iloc[-1]
    avg_loss = loss.rolling(period).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


def calc_ma(prices: pd.Series, period: int) -> float:
    if len(prices) < period:
        return prices.mean()
    return prices.tail(period).mean()


def classify_stock(hist_df: pd.DataFrame, pe_ttm: float = None, pb: float = None) -> dict:
    if hist_df is None or len(hist_df) < 20:
        return {'side': '观望', 'right_score': 0, 'left_score': 0,
                'signals': ['数据不足'], 'signal_strength': '观望',
                'ma5': 0, 'ma10': 0, 'ma20': 0, 'rsi14': 50, 'price_position': 50}

    closes = hist_df['close']
    volumes = hist_df['volume'] if 'volume' in hist_df.columns else pd.Series([1]*len(hist_df))

    ma5  = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)
    rsi  = calc_rsi(closes)

    price_now = closes.iloc[-1]
    high_30 = closes.tail(30).max() if len(closes) >= 30 else closes.max()
    low_30  = closes.tail(30).min() if len(closes) >= 30 else closes.min()
    price_position = (price_now - low_30) / (high_30 - low_30 + 1e-9)

    right_score = 0
    left_score  = 0
    signals     = []

    # ---- 右侧得分 ----
    if price_now > ma20:
        right_score += 1; signals.append('价格站上MA20')
    if ma5 > ma10 > ma20:
        right_score += 2; signals.append('均线多头排列')
    if price_position > 0.7:
        right_score += 1; signals.append('价格处于30日高位')
    if len(closes) >= 3 and closes.iloc[-1] > closes.iloc[-3]:
        right_score += 1; signals.append('近3日持续上涨')
    if len(hist_df) >= 5:
        recent5 = hist_df.tail(5)
        chg_col = recent5.get('change_pct', pd.Series([0]*5))
        up_vol = recent5[chg_col > 0]['volume'].sum() if 'volume' in recent5 else 0
        dn_vol = recent5[chg_col <= 0]['volume'].sum() if 'volume' in recent5 else 0
        if up_vol > dn_vol:
            right_score += 1; signals.append('量能配合上涨')
    # MACD-like: 短期动量
    if len(closes) >= 10:
        ema5  = closes.ewm(span=5, adjust=False).mean().iloc[-1]
        ema10 = closes.ewm(span=10, adjust=False).mean().iloc[-1]
        if ema5 > ema10:
            right_score += 1; signals.append('EMA5上穿EMA10')

    # ---- 左侧得分 ----
    if price_now < ma20:
        left_score += 1; signals.append('价格在MA20下方')
    if price_position < 0.3:
        left_score += 1; signals.append('价格处于30日低位')
    if rsi < 35:
        left_score += 2; signals.append(f'RSI严重超跌({rsi:.1f})')
    elif rsi < 45:
        left_score += 1; signals.append(f'RSI偏弱({rsi:.1f})')
    if len(closes) >= 5:
        recent = closes.tail(5)
        if abs(recent.iloc[-1] - recent.iloc[-2]) < abs(recent.iloc[-3] - recent.iloc[-4]):
            left_score += 1; signals.append('跌幅收窄企稳')
    if len(hist_df) >= 5:
        recent5 = hist_df.tail(5)
        chg_col = recent5.get('change_pct', pd.Series([0]*5)) if 'change_pct' in recent5.columns else pd.Series([0]*5)
        dn_days = recent5[chg_col < 0]
        if len(dn_days) >= 2 and 'volume' in dn_days.columns:
            if dn_days['volume'].is_monotonic_decreasing:
                left_score += 1; signals.append('下跌缩量')
    # 60日新低
    low_60 = closes.tail(60).min() if len(closes) >= 60 else closes.min()
    if price_now <= low_60 * 1.02:
        left_score += 1; signals.append('接近60日低点')

    # ---- 分类 ----
    if right_score >= 3 and right_score >= left_score:
        side = '右侧'
    elif left_score >= 3 and left_score > right_score:
        side = '左侧'
    else:
        side = '观望'

    # ---- 价值区间（需PE数据）----
    value_tag = None
    if pe_ttm is not None and pe_ttm > 0:
        if pe_ttm < 10:
            value_tag = '成熟股价值空间'
            signals.append(f'PE仅{pe_ttm:.1f}倍')
        elif pe_ttm < 20:
            value_tag = 'PE合理'
            signals.append(f'PE{pe_ttm:.1f}倍')
        else:
            value_tag = f'PE{pe_ttm:.0f}倍偏高'
    elif pb is not None and pb > 0 and pb < 1:
        value_tag = 'PB<1破净值'
        signals.append(f'PB{pb:.2f}低于净资产')

    # ---- 信号强度 ----
    if side == '右侧':
        if right_score >= 6:
            signal_strength = '⚡强烈买入'
        elif right_score >= 4:
            signal_strength = '✅买入'
        else:
            signal_strength = '📈关注买入'
    elif side == '左侧':
        if left_score >= 6:
            signal_strength = '🔴强烈卖出'
        elif left_score >= 4:
            signal_strength = '⚠️卖出观察'
        else:
            signal_strength = '🔻弱势关注'
    else:
        if max(right_score, left_score) >= 2:
            signal_strength = '🔍待确认'
        else:
            signal_strength = '😴横盘观望'

    return {
        'side':           side,
        'right_score':    right_score,
        'left_score':     left_score,
        'signals':        signals,
        'signal_strength': signal_strength,
        'value_tag':      value_tag,
        'ma5':            round(ma5,  3),
        'ma10':           round(ma10, 3),
        'ma20':           round(ma20, 3),
        'rsi14':          rsi,
        'price_position': round(price_position * 100, 1),
    }
