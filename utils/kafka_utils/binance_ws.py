import ssl
import json
import time
import asyncio
import logging
import websockets
from collections import deque
from aiokafka import AIOKafkaProducer

BINANCE_WS_URL = "wss://fstream.binance.com/ws/btcusdt@trade"

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


async def producer_ws_stream(producer: AIOKafkaProducer, topic: str, key: str):
    fallback_buffer = deque(maxlen=10000)
    asyncio.create_task(retry_fallback_buffer(producer, topic, fallback_buffer))
    ws_retry_wait = 1

    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL, ssl=ssl_context) as ws:
                logging.info(f"{topic}: WebSocket connection established")
                ws_retry_wait = 1
                async for msg in ws:
                    trade = json.loads(msg)
                    trade_data = {
                        "trade_id": trade["t"],
                        "symbol": trade["s"],
                        "price": trade["p"],
                        "quantity": trade["q"],
                        # "buyer_order_id": trade.get("b"),
                        # "seller_order_id": trade.get("a"),
                        "is_buyer_maker": trade["m"],
                        "event_type": trade.get("e"),
                        "produced_time": time.time() * 1000,
                        # "event_time": trade["E"],
                        "trade_time": trade["T"]
                    }

                    try:
                        await producer.send_and_wait(
                            topic=topic,
                            key=str(trade_data[key]),
                            value=trade_data
                        )
                    except Exception as e:
                        logging.error(f"{topic}: Kafka error: {e}")
                        fallback_buffer.append((str(trade[key]), trade))
        except Exception as ws_error:
            logging.error(f"{topic}: WebSocket error: {ws_error}. Reconnecting in 5 seconds...")
            await asyncio.sleep(ws_retry_wait)
            ws_retry_wait = min(ws_retry_wait + 2, 30)


async def retry_fallback_buffer(producer, topic, fallback_buffer):
    while True:
        if fallback_buffer:
            logging.info(f"{topic}: Retrying {len(fallback_buffer)} buffered trades...")
            retry_success = []
            for k, t in list(fallback_buffer):
                try:
                    await producer.send_and_wait(topic=topic, key=k, value=t)
                    retry_success.append((k, t))
                except Exception as retry_err:
                    logging.error(f"{topic}: Retry failed: {retry_err}")
                    break  # if Kafka is still down, stop retrying and wait 1s

            for item in retry_success:
                fallback_buffer.remove(item)

        await asyncio.sleep(1)
