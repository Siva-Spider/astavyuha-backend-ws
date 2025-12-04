
import pandas as pd
import numpy as np
import ta
def supertrend(df, period=8, multiplier=3.2):
    # --- ATR ---
    atr = ta.volatility.AverageTrueRange(
        df['high'], df['low'], df['close'], window=period
    ).average_true_range()

    hl2 = (df['high'] + df['low']) / 2

    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr

    # Prepare arrays
    supertrend = [True] * len(df)
    final_upper = upperband.copy()
    final_lower = lowerband.copy()

    # --- MAIN LOOP (no deprecated behavior) ---
    for i in range(1, len(df)):
        curr_close = df['close'].iat[i]
        prev_close = df['close'].iat[i - 1]

        # Trend flip
        if curr_close > final_upper.iat[i - 1]:
            supertrend[i] = True
        elif curr_close < final_lower.iat[i - 1]:
            supertrend[i] = False
        else:
            # inherit trend
            supertrend[i] = supertrend[i - 1]

            # adjust lower band if trend is bullish
            if supertrend[i] and final_lower.iat[i] < final_lower.iat[i - 1]:
                final_lower.iat[i] = final_lower.iat[i - 1]

            # adjust upper band if trend is bearish
            if not supertrend[i] and final_upper.iat[i] > final_upper.iat[i - 1]:
                final_upper.iat[i] = final_upper.iat[i - 1]

    # Create final supertrend line
    df['supertrend'] = [
        final_lower.iat[i] if supertrend[i] else final_upper.iat[i]
        for i in range(len(df))
    ]

    return df['supertrend']
    
def all_indicators(df,strategy):
    df = df.copy()
    tf =df.copy()

    # ========================
    # ✅ ATR (for Supertrend & ADX)
    # ========================
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    
    df['Supertrend'] = supertrend(df, period=8, multiplier=3.2)

    # ========================
    # ✅ MACD (12,26,9)
    # ========================
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = df['MACD'] - df['MACD_signal']


    # ========================
    # ✅ ADX (14) with EMA 21 smoothing
    # ========================
    period = 14
    tf['upMove'] = tf['high'].diff()
    tf['downMove'] = tf['low'].diff() * -1

    tf['+DM'] = np.where((tf['upMove'] > tf['downMove']) & (tf['upMove'] > 0), tf['upMove'], 0.0)
    tf['-DM'] = np.where((tf['downMove'] > tf['upMove']) & (tf['downMove'] > 0), tf['downMove'], 0.0)

    tr1 = tf['high'] - tf['low']
    tr2 = (tf['high'] - tf['close'].shift()).abs()
    tr3 = (tf['low'] - tf['close'].shift()).abs()
    tf['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Wilder's smoothing
    tf['TR14'] = tf['TR'].ewm(alpha=1 / period, adjust=False).mean()
    tf['+DM14'] = tf['+DM'].ewm(alpha=1 / period, adjust=False).mean()
    tf['-DM14'] = tf['-DM'].ewm(alpha=1 / period, adjust=False).mean()

    tf['+DI14'] = 100 * (tf['+DM14'] / tf['TR14'])
    tf['-DI14'] = 100 * (tf['-DM14'] / tf['TR14'])

    tf['DX'] = (100 * (abs(tf['+DI14'] - tf['-DI14']) / (tf['+DI14'] + tf['-DI14'])))
    df['ADX'] = tf['DX'].ewm(alpha=1 / period, adjust=False).mean()

    # Optional: EMA 21 smoothing
    df['ADX_EMA21'] = df['ADX'].ewm(span=21, adjust=False).mean()

    # ========================
    # ✅ Williams %R (14)
    # ========================
    window = 14
    high14 = df['high'].rolling(window).max()
    low14 = df['low'].rolling(window).min()
    df['WillR_14'] = (high14 - df['close']) / (high14 - low14) * -100
    # ========================
    # ✅ EMA 5, EMA 15
    # ========================
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()

    df.dropna(inplace=True)
    df = df.round(2)
    base_cols = ['datetime', 'open', 'high', 'low', 'close']
    indicator_cols = []

    if strategy == "ADX_MACD_WillR_Supertrend":
        indicator_cols = ['Supertrend', 'MACD', 'MACD_signal', 'ADX', 'WillR_14', 'ema10', 'ema20']
    elif strategy == "Ema10_Ema20_Supertrend":
        indicator_cols = ['ema10', 'ema20', 'Supertrend']
    elif strategy == "Ema10_Ema20_MACD_Supertrend":
        indicator_cols = ['ema10', 'ema20', 'MACD', 'MACD_signal', 'Supertrend']
    else:
        # default: return all computed indicators plus base columns
        indicator_cols = ['Supertrend', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'ADX_EMA21', 'WillR_14', 'ema10', 'ema20']

        # ensure requested columns exist
    out_cols = [c for c in base_cols if c in df.columns] + [c for c in indicator_cols if c in df.columns]
    result = df.loc[:, out_cols].copy()

    # Round to 2 decimals for presentation (optional)
    # result = result.round(2)

    return result
