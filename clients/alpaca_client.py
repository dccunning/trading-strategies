import os
import time
from datetime import datetime
from typing import List, Dict

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame


# NOT IN USE
class AlpacaClient:
    def __init__(self):
        self.api_key = os.environ["ALPACA_API_KEY"]
        self.secret_key = os.environ["ALPACA_SECRET_KEY"]
        self.client = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.rate_limit = 200

    def get_bars(
        self,
        tickers: List[str],
        timeframe: TimeFrame,
        start: datetime,
        end: datetime,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetches historical bar data for a list of stock tickers over a given time range.

        Parameters:
            tickers (List[str]): List of stock symbols
            timeframe (TimeFrame): Time granularity of the bars
            start (datetime): Start time the data
            end (datetime): End time for the data

        Returns:
            Dict[str, pd.DataFrame]: A dictionary where each key is a ticker symbol and each value
                                     is a table containing its historical OHLCV bar data.
        """
        data = {}
        batch_size = 180
        request_count = 0
        start_time = time.time()

        for i, symbol in enumerate(tickers):
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
            )
            bars = self.client.get_stock_bars(request).df
            if not bars.empty:
                data[symbol] = bars.xs(symbol, level="symbol")
            request_count += 1

            # Enforce rate limit
            if request_count >= batch_size:
                elapsed = time.time() - start_time
                sleep_time = max(60 - elapsed, 0)
                print(f"Sleeping {sleep_time:.2f}s to respect rate limit")
                time.sleep(sleep_time)
                start_time = time.time()
                request_count = 0

        return data


if __name__ == "__main__":
    from dotenv import load_dotenv
    from datetime import datetime, timedelta
    load_dotenv()

    client = AlpacaClient()
    tickers = ["AAPL", "MSFT", "NVDA"]
    start = datetime.now() - timedelta(days=180)
    end = datetime.now()

    data = client.get_bars(
        tickers=tickers,
        timeframe=TimeFrame.Hour,
        start=start,
        end=end
    )

    for symbol, df in data.items():
        print(f"\n--- {symbol} ({len(df)} bars) ---")
        print(df.head().to_string())