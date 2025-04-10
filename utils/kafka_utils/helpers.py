import os
import json
import time
import math
import logging
import asyncio
from datetime import datetime
from aiokafka import AIOKafkaProducer
from typing import List, Callable, Awaitable


async def publish_to_kafka_async(
        producer: AIOKafkaProducer,
        topic: str,
        data: List[dict],
        key: str
):
    """Publish data to a Kafka producer asynchronously.

    :param producer: The producer object to publish with.
    :param topic: The Kafka topic to send data to.
    :param data: List of payload data to send.
    :param key: The key value used by Kafka to partition data.
    """
    try:
        if data:
            for payload in data:
                payload = {**payload, 'produced_time': time.time() * 1000}
                await producer.send_and_wait(
                    topic=topic,
                    key=str(payload[key]),
                    value=payload
                )
    except Exception as e:
        logging.error(f"{topic}: Error sending message: {e}")


async def producer_async(
    frequency: float,
    topic: str,
    key_field: str,
    get_data_func: Callable[[], Awaitable[List[dict]]],
    log_interval: int = 3600
):
    """
    Generic Kafka producer that publishes data at a given interval.

    :param frequency: Time in seconds between each data retrieval.
    :param topic: Kafka topic to publish to.
    :param key_field: Key in each dict used for Kafka partitioning.
    :param get_data_func: Async function that returns List[dict] for publishing.
    :param log_interval: Number of seconds to wait between logging the number of published data points.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await producer.start()

    last_log_time = time.time()
    data_published = 0

    try:
        next_run = math.ceil(time.time() / frequency) * frequency

        while True:
            now = time.time()
            if now > next_run + frequency:
                missed = int((now - next_run) // frequency)
                next_run += (missed + 1) * frequency
                logging.warning(f"{topic}: Skipped {missed + 1} intervals, catching up at {datetime.fromtimestamp(next_run)}")

            await asyncio.sleep(max(0, next_run - time.time()))
            data = await get_data_func()
            await publish_to_kafka_async(producer, topic, data, key_field)
            next_run += frequency
            data_published += len(data)

            if now - last_log_time >= log_interval:
                logging.info(f"{topic}: Published {data_published} records")
                last_log_time = now
                data_published = 0
    finally:
        await producer.stop()
