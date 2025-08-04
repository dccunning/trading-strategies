import requests
import pandas as pd

#  Go long when price closes above 50-EMA (close) and stay out (or short) when it’s below


def get_binance_klines(symbol='BTCUSDT', interval='1d', limit=150):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_ema_strategy_data(days_back: int = 100):
    """
    Fetch enough data to compute a stable 50-day EMA

    :param days_back: Number of days to check for
    :return: DataFrame of price changes
    """
    data = get_binance_klines(limit=days_back+50)

    df = pd.DataFrame({
        'timestamp': [pd.to_datetime(entry[0], unit='ms') for entry in data],
        'close': [float(entry[4]) for entry in data]
    })

    df['EMA_50'] = df['close'].ewm(span=50, adjust=False, min_periods=50).mean()
    df = df.dropna(subset=['EMA_50']).reset_index(drop=True)

    # Generate entry/exit signals based on crossing
    df['prev_close'] = df['close'].shift(1)
    df['prev_ema'] = df['EMA_50'].shift(1)
    df['signal'] = '-'
    # Go Long when price crosses above EMA
    go_long = (df['prev_close'] <= df['prev_ema']) & (df['close'] > df['EMA_50'])
    # Go Short when price crosses below EMA
    go_short = (df['prev_close'] >= df['prev_ema']) & (df['close'] < df['EMA_50'])
    df.loc[go_long, 'signal'] = 'Go Long'
    df.loc[go_short, 'signal'] = 'Go Short'

    # Print rows with colored values
    for _, row in df.iterrows():
        price = row['close']
        ema = row['EMA_50']
        signal = row['signal']

        # Color logic: close green if above EMA (EMA red), else reverse
        if price > ema:
            price_str = f"\033[32m{price:.2f}\033[0m"
            ema_str   = f"\033[31m{ema:.2f}\033[0m"
        else:
            price_str = f"\033[31m{price:.2f}\033[0m"
            ema_str   = f"\033[32m{ema:.2f}\033[0m"

        if signal == 'Go Long':
            signal_str = f"\033[32m{signal}\033[0m"
            last_long = price
        elif signal == 'Go Short':
            signal_str = f"\033[31m{signal}\033[0m"
            last_short = price
        else:
            signal_str = signal

        print(f"{row['timestamp'].date()}  Close: {price_str}  EMA_50: {ema_str}  Signal: {signal_str}")

    return df


if __name__ == "__main__":
    df = get_ema_strategy_data(400)
