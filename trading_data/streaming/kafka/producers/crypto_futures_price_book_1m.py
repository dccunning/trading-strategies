import logging
import asyncio
from kafka_streams.utils.helpers import producer_async
from kafka_streams.utils.binance_crypto import get_futures_price_bookTicker

TOPIC = 'crypto-futures-price-book-1m'
PRICE_DELAY_IN_SECONDS = 60.0
# Returns all symbols by default

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

asyncio.run(
    producer_async(
        frequency=PRICE_DELAY_IN_SECONDS,
        topic=TOPIC,
        key_field="symbol",
        get_data_func=lambda: get_futures_price_bookTicker()
    )
)
