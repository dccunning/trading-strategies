import httpx
import asyncio
import logging
from httpx import AsyncClient
from aiokafka import AIOKafkaProducer


logging.getLogger("httpx").setLevel(logging.WARNING)


async def get_data_and_produce(client: AsyncClient, producer: AIOKafkaProducer, topic: str, key: str, url: str):
    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logging.warning(f"{topic} API error: {e}")
        return

    for result in data:
        try:
            await producer.send_and_wait(
                topic=topic,
                key=str(result.get(key)),
                value=result
            )
        except Exception as e:
            logging.error(f"{topic} Kafka error sending {result.get(key)}: {e}")


async def producer_stream_api_book_price(producer: AIOKafkaProducer, topic: str, key: str, url: str, frequency: float):
    async with httpx.AsyncClient() as client:
        while True:
            asyncio.create_task(get_data_and_produce(client, producer, topic, key, url))
            await asyncio.sleep(frequency)
