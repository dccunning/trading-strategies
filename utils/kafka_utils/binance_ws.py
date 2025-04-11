import ssl
import json
import time
import asyncio
import logging
import websockets
from collections import deque
from aiokafka import AIOKafkaProducer

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


async def websocket_producer_stream(url: str, response_mapping: dict, producer: AIOKafkaProducer, topic: str, key: str):
    fallback_buffer = deque(maxlen=10000)
    asyncio.create_task(retry_fallback_buffer(producer, topic, fallback_buffer))
    ws_retry_wait = 1

    while True:
        try:
            async with websockets.connect(url, ssl=ssl_context) as ws:
                logging.info(f"{topic}: WebSocket connection established")
                ws_retry_wait = 1
                async for msg in ws:
                    trade = json.loads(msg)
                    trade_data = {
                        key: trade[value] for key, value in response_mapping.items()
                    }
                    trade_data["produced_time"] = time.time() * 1000

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
