import os
import json
import time
import math
import logging
import asyncio
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
            tasks = []
            for payload in data:
                task = asyncio.create_task(
                    producer.send_and_wait(
                        topic=topic,
                        key=payload[key],
                        value=payload
                    )
                )
                tasks.append(task)
                logging.info(f"Sent: {payload}")

            await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Error sending message: {e}")


async def producer_async(
    frequency: float,
    topic: str,
    key_field: str,
    get_data_func: Callable[[], Awaitable[List[dict]]]
):
    """
    Generic Kafka producer that publishes data at a given interval.

    :param frequency: Time between each data retrieval.
    :param topic: Kafka topic to publish to.
    :param key_field: Key in each dict used for Kafka partitioning.
    :param get_data_func: Async function that returns List[dict] for publishing.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await producer.start()

    try:
        next_run = math.ceil(time.time() / frequency) * frequency

        while True:
            now = time.time()
            if now > next_run + frequency:
                missed = int((now - next_run) // frequency)
                next_run += (missed + 1) * frequency
                logging.warning(f"⏩ Skipped {missed + 1} intervals, catching up at {next_run}")

            await asyncio.sleep(max(0, next_run - time.time()))
            data = await get_data_func()
            await publish_to_kafka_async(producer, topic, data, key_field)
            next_run += frequency
    finally:
        await producer.stop()
