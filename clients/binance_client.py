import time
import requests
import pandas as pd
from datetime import datetime

BASE_URL = "https://api.binance.com"


def to_millis(x):
    """
    Convert datetime or int to milliseconds since epoch.
    If already an int (ms), returns it unchanged.
    """
    if isinstance(x, int):
        return x
    elif isinstance(x, datetime):
        return int(x.timestamp() * 1000)
    else:
        raise TypeError(f"Invalid time format: expected datetime or int (ms), got {type(x)}")


def get_ohlcv(symbol, interval="1m", start=None, end=None, limit=1000):
    """
    Fetch all OHLCV data between two datetimes (max 1000 rows).

    :param symbol: ex: "BTCUSDT"
    :param interval: ex: "1m", "1h"
    :param start: datetime.datetime OR ms
    :param end: datetime.datetime OR ms (optional)
    :param limit: number of rows
    :return: DataFrame
    """
    url = f"{BASE_URL}/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    if start:
        params["startTime"] = to_millis(start)
    if end:
        params["endTime"] = to_millis(end)

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume"]]
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df.set_index("open_time", inplace=True)

    float_cols = ["open", "high", "low", "close", "volume"]
    df[float_cols] = df[float_cols].astype(float)
    return df


def get_all_ohlcv(symbol, interval="1m", start=None, end=None, save_to_path=None):
    """
    Fetch all OHLCV data between two datetimes (with auto-pagination).

    :param symbol: e.g. "BTCUSDT"
    :param interval: e.g. "1m", "1h"
    :param start: datetime.datetime (required)
    :param end: datetime.datetime (optional)
    :param save_to_path: path to save data to as csv
    :return: pd.DataFrame with OHLCV data
    """
    if start is None:
        raise ValueError("Start time must be provided as a datetime.")

    df_list = []
    start_time = to_millis(start)
    end_time = to_millis(end) if end else None
    max_limit = 1000

    while True:
        df = get_ohlcv(symbol, interval, start=start_time, end=end_time, limit=max_limit)
        if df.empty:
            break

        df_list.append(df)
        last_time = int(df.index[-1].timestamp() * 1000)

        # Stop if we got fewer than max_limit rows (end of data)
        if len(df) < max_limit:
            break

        start_time = last_time + 1

    if save_to_path:
        df.to_csv(save_to_path)

    return pd.concat(df_list)


if __name__ == "__main__":
    from datetime import datetime, timedelta

    start_dt = datetime.utcnow() - timedelta(days=100)
    end_dt = datetime.utcnow()

    df = get_all_ohlcv("BTCUSDT", interval="1h", start=start_dt, end=end_dt)
    df = get_ohlcv("BTCUSDT", interval="1h", start=start_dt, end=end_dt)

    print(df.info())
