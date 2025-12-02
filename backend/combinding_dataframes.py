from collections import deque
import pandas as pd

def combinding_dataframes(*dfs):

    candle_buffer = []

    # Collect candles from each DF
    for df in dfs:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            continue

        for dt, row in df.iterrows():
            candle_buffer.append({
                'datetime': dt,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
            })

    if not candle_buffer:
        return pd.DataFrame()

    # Convert to DataFrame
    final_df = pd.DataFrame(candle_buffer)

    # Convert column to datetime
    final_df['datetime'] = pd.to_datetime(final_df['datetime'])

    # Remove duplicates based on datetime
    final_df.drop_duplicates(subset=['datetime'], keep='last', inplace=True)

    # Sort by datetime
    final_df.sort_values('datetime', inplace=True)

    # Set datetime as index
    final_df.set_index('datetime', inplace=True)

    return final_df
