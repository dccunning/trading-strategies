import httpx
import asyncio
import logging
from typing import List
from decimal import Decimal
from datetime import datetime, timezone

logging.getLogger("httpx").setLevel(logging.WARNING)

futures_price_book_1s = """
CREATE TABLE crypto.futures_price_book_1s (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    last_trade_price NUMERIC,
    bid_price NUMERIC,
    ask_price NUMERIC,
    mid_price NUMERIC,
    bid_qty NUMERIC,
    ask_qty NUMERIC,
    ts_last_trade_price TIMESTAMPTZ,
    ts_book_ticker TIMESTAMPTZ,
    PRIMARY KEY (symbol, timestamp)
);
"""
futures_price_book_1m = """
CREATE TABLE crypto.futures_price_book_1m (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    last_trade_price NUMERIC,
    bid_price NUMERIC,
    ask_price NUMERIC,
    mid_price NUMERIC,
    bid_qty NUMERIC,
    ask_qty NUMERIC,
    ts_last_trade_price TIMESTAMPTZ,
    ts_book_ticker TIMESTAMPTZ,
    PRIMARY KEY (symbol, timestamp)
);
"""
futures_mark_index_500ms = """
CREATE TABLE crypto.futures_mark_index_500ms (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    mark_price NUMERIC,
    index_price NUMERIC,
    last_funding_rate NUMERIC,
    interest_rate NUMERIC,
    ts_next_funding_time TIMESTAMPTZ,
    ts_premium_index TIMESTAMPTZ,
    PRIMARY KEY (symbol, timestamp)
);
"""


def get_decimal_places(value: float) -> int:
    d = Decimal(str(value)).normalize()
    # If it's scientific notation (e.g. 1E-8), force it into decimal string
    if 'E' in str(d):
        d = Decimal(d.to_eng_string())
    parts = str(d).split('.')
    return len(parts[1]) if len(parts) == 2 else 0


async def get_futures_price_bookTicker(symbols: List[str] = None, timeout: float = 0.5) -> List[dict]:
    """Returns a list of row data from the last_price and book_ticker endpoints of Binance.com.


    :param symbols: A list of crypto coin conversion strings. If None then all symbols are returned.
    :param timeout: The max time in seconds to spend waiting for an API response.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            import time
            start = time.time()
            logging.log(logging.INFO,f'price_bookTicker ({str(len(symbols)) if symbols else 'all'}) - request start - {datetime.fromtimestamp(start)}')

            # price_response, book_response = await asyncio.gather(
            #     client.get("https://fapi.binance.com/fapi/v1/ticker/price"),
            #     client.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
            # )
            task1 = asyncio.create_task(client.get("https://fapi.binance.com/fapi/v1/ticker/price"))
            await asyncio.sleep(0.1)  # Intentional 100ms delay
            task2 = asyncio.create_task(client.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker"))
            price_response, book_response = await asyncio.gather(task1, task2)
            await asyncio.sleep(0.1)

            all_prices_ts = datetime.now(timezone.utc).isoformat()

            all_prices = price_response.json()
            book_tickers = book_response.json()
            logging.log(logging.INFO, f'price_bookTicker ({str(len(symbols)) if symbols else 'all'}) - request done - {datetime.fromtimestamp(time.time())} - ({time.time() - start})')

            all_prices_map = {
                item["symbol"]: {
                    "last_price": float(item["price"]),
                    "ts_price": datetime.fromtimestamp(item["time"] / 1000, tz=timezone.utc).isoformat()
                }
                for item in all_prices
                if symbols is None or item["symbol"] in symbols
            }
            book_tickers_map = {
                item["symbol"]: {
                    "ts_book_ticker": datetime.fromtimestamp(item["time"] / 1000, tz=timezone.utc).isoformat(),
                    "bidPrice": float(item["bidPrice"]),
                    "askPrice": float(item["askPrice"]),
                    "bidQty": float(item["bidQty"]),
                    "askQty": float(item["askQty"])
                }
                for item in book_tickers
                if symbols is None or item["symbol"] in symbols
            }

            price_ticks = []
            for symbol in (symbols if symbols is not None else all_prices_map.keys()):
                trade_price = all_prices_map.get(symbol, {})
                book = book_tickers_map.get(symbol, {})
                last_price = trade_price.get("last_price")
                ts_price = trade_price.get("ts_price")
                ts_book_ticker = book.get("ts_book_ticker")
                bidPrice = book.get("bidPrice")
                askPrice = book.get("askPrice")
                bidQty = book.get("bidQty")
                askQty = book.get("askQty")
                midPrice = (bidPrice + askPrice) / 2 if bidPrice is not None and askPrice is not None else None

                if midPrice is not None:
                    mid_decimals = max(get_decimal_places(bidPrice), get_decimal_places(askPrice)) + 1
                    midPrice = round(midPrice, mid_decimals)

                price_ticks.append({
                    "symbol": symbol,
                    "timestamp": all_prices_ts,
                    "last_trade_price": last_price,
                    "bid_price": bidPrice,
                    "ask_price": askPrice,
                    "mid_price": midPrice,
                    "bid_qty": bidQty,
                    "ask_qty": askQty,
                    "ts_last_trade_price": ts_price,
                    "ts_book_ticker": ts_book_ticker
                })

            return price_ticks
    except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
        logging.warning(f"get_futures_price_bookTicker (timeout={timeout}): {e}")
        return []


async def get_futures_mark_index(symbols: List[str], timeout: float = 0.3) -> List[dict]:
    """Returns a list of row data from the futures premiumIndex endpoint of Binance.com.

    :param symbols: A list of crypto coin conversion strings.
    :param timeout: The max time in seconds to spend waiting for an API response.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            import time
            start = time.time()
            logging.log(logging.INFO,f'futures_mark_index ({str(len(symbols)) if symbols else 'all'}) - request start - {datetime.fromtimestamp(start)}')

            responses = await asyncio.gather(*[
                client.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}")
                for symbol in symbols
            ])
            all_prices_ts = datetime.now(timezone.utc).isoformat()
            logging.log(logging.INFO, f'futures_mark_index ({str(len(symbols)) if symbols else 'all'}) - request done - {datetime.fromtimestamp(time.time())} - ({time.time() - start})')
            await asyncio.sleep(0.1)
            prices = [
                {
                    "symbol": data["symbol"],
                    "timestamp": all_prices_ts,
                    "mark_price": float(data["markPrice"]),
                    "index_price": float(data["indexPrice"]),
                    "last_funding_rate": float(data["lastFundingRate"]),
                    "interest_rate": float(data["interestRate"]),
                    "ts_next_funding_time": datetime.fromtimestamp(data["nextFundingTime"] / 1000,
                                                                   tz=timezone.utc).isoformat(),
                    "ts_premium_index": datetime.fromtimestamp(data["time"] / 1000, tz=timezone.utc).isoformat(),
                }
                for response in responses
                if (data := response.json())
            ]

            return prices
    except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
        logging.warning(f"get_futures_mark_index (timeout={timeout}): {e}")
        return []
