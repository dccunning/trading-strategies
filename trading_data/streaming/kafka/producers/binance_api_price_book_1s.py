import logging
import asyncio
from utils.kafka_utils.helpers import producer_async
from utils.kafka_utils.binance_api import get_futures_price_bookTicker

TOPIC = 'binance-api-price-book-1s'
PRICE_DELAY_IN_SECONDS = 1.0
REQUEST_TIMEOUT = 0.5
SYMBOLS = ["ETHUSDT", "BTCUSDT", "XMRUSDT"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

asyncio.run(
    producer_async(
        frequency=PRICE_DELAY_IN_SECONDS,
        topic=TOPIC,
        key_field="symbol",
        get_data_func=lambda: get_futures_price_bookTicker(symbols=SYMBOLS, timeout=REQUEST_TIMEOUT),
        log_interval=3600
    )
)
