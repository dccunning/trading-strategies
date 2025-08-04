import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf


def big_hourly_moves_df(stocks: list, x_pct: float) -> pd.DataFrame:
    """
    Finds large hourly price moves (>= x_pct) for given stocks in the last hour.
    """
    pass


def historical_big_hourly_moves_df(stocks: list, time_start: datetime, x_pct: float, max_hold_days: int) -> pd.DataFrame:
    """
    Finds large hourly price moves (>= x_pct) for given stocks since time_start.


    Returns a DataFrame with columns:
        stock, time, increase_pct, prev_close, close, high_0d, .., low_0d, .., gain_0d, .., loss_0d
        gain_max, loss_max
    """
    all_moves = []
    future_offsets = list(range(max_hold_days+1))

    # compute daily window once: from one day before start through today + max offset
    daily_start = time_start - timedelta(days=1)
    daily_end   = datetime.now() + timedelta(days=max(future_offsets))

    for symbol in stocks:
        # --- 1) Hourly data ---
        hourly = yf.download(
            symbol,
            start=time_start.strftime("%Y-%m-%d"),
            interval="60m",
            progress=False
        )
        if hourly.empty or 'Close' not in hourly:
            continue

        # Flatten MultiIndex if needed
        if isinstance(hourly.columns, pd.MultiIndex):
            hourly.columns = hourly.columns.get_level_values(0)

        hourly.index = pd.to_datetime(hourly.index)

        # Localize/convert timezone to US/Eastern
        if hourly.index.tz is None:
            hourly.index = hourly.index.tz_localize('UTC')
        hourly.index = hourly.index.tz_convert('America/New_York')
        hourly = hourly.sort_index()

        # --- 2) Daily data ---
        daily = yf.download(
            symbol,
            start=daily_start.strftime("%Y-%m-%d"),
            end=daily_end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False
        )
        if daily.empty or 'High' not in daily:
            continue

        if isinstance(daily.columns, pd.MultiIndex):
            daily = daily.xs(symbol, axis=1, level=1)
        daily.index = pd.to_datetime(daily.index)
        daily = daily.sort_index()

        trade_dates = [ts.date() for ts in daily['High'].index]

        def get_future_price(type: str, ts: pd.Timestamp, offset: int):
            """ Helper to get the Nth next market-day price

            type: 'High', 'Low', 'Close', 'Open'
            ts: Timestamp for the future metric
            offset: Nth next market day
            """
            md = ts.date()
            if md not in trade_dates:
                return None
            pos = trade_dates.index(md)
            tgt = pos + offset
            if 0 <= tgt < len(daily[type]):
                return round(daily[type].iloc[tgt], 2)
            return None

        # --- 3) Scan for big hourly moves ---
        for i in range(1, len(hourly)):
            prev_close = round(hourly['Close'].iloc[i - 1], 2)
            curr_close = round(hourly['Close'].iloc[i], 2)

            increase_ratio = (curr_close - prev_close) / prev_close

            if increase_ratio >= x_pct:
                ts = hourly.index[i]
                if ts >= pd.Timestamp('2025-06-27').tz_localize('America/New_York'):
                    continue

                daily_highs = [round(get_future_price('High', ts, x), 2) for x in range(max_hold_days+1)]
                daily_lows = [round(get_future_price('Low', ts, x), 2) for x in range(max_hold_days+1)]

                daily_gains = [round(daily_highs[idx] / curr_close - 1, 3) * 100 for idx in range(len(daily_highs))]
                daily_losses = [round(daily_lows[idx] / curr_close - 1, 3) * 100 for idx in range(len(daily_highs))]

                row = {
                    "stock": symbol,
                    "time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "increase_pct": round(increase_ratio, 3) * 100,
                    "prev_close": prev_close,
                    "close": curr_close,
                }

                for idx in range(max_hold_days+1):
                    row[f"high_{idx}d"] = daily_highs[idx]
                    row[f"low_{idx}d"] = daily_lows[idx]
                    row[f"gain_{idx}d"] = daily_gains[idx]
                    row[f"loss_{idx}d"] = daily_losses[idx]

                all_moves.append(row)

    df = pd.DataFrame(all_moves)

    gain_cols = [f'gain_{i}d' for i in range(max_hold_days+1)]
    loss_cols = [f'loss_{i}d' for i in range(max_hold_days+1)]

    df['gain_max'] = df[gain_cols].max(axis=1)
    df['loss_max'] = df[loss_cols].min(axis=1)

    return df

# TODO: (Trading strategy execution)
#  1. Create email notification when a big stock is trending up >3% in an hour
#  & link news on it & show strategy
#  2. Manually check price chart & options charts
#  3. Decide if confident in buying options & buying CDF position for short term gain

# TODO: (Backtesting)
#  Why: See how sudden price increases predict a short term uptrend & by how much
#  What: Predict the max of daily highs to TP at, and respective lows to SL at


if __name__ == "__main__":
    result_df = historical_big_hourly_moves_df(
        stocks=['NKE', 'AAPL', 'MSFT', 'NVDA', 'META', 'TSLA', 'BAC', 'JPM', 'AMD', 'NFLX', 'GOOGL'],
        time_start=datetime(2024, 1, 1, 0, 0, 0),
        x_pct=0.02,
        max_hold_days=1
    )
    print(result_df.to_string())

    print('Avg hourly increase:', round(result_df['increase_pct'].mean(numeric_only=True), 3))
    print('Avg best gain:', round(result_df['gain_max'].mean(numeric_only=True), 3))
    print('Avg worst loss:', round(result_df['loss_max'].mean(numeric_only=True), 3))

    best_over_3 = (result_df['gain_max'] > 3).sum() / len(result_df)
    print('Best gain >3%', round(best_over_3, 3) * 100)

    best_over_4 = (result_df['gain_max'] > 4).sum() / len(result_df)
    print('Best gain >4%', round(best_over_4, 3) * 100)

    best_over_5 = (result_df['gain_max'] > 5).sum() / len(result_df)
    print('Best gain >5%', round(best_over_5, 3) * 100)
