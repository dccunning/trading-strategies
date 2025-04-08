FROM python:3.12-slim

WORKDIR /

COPY trading_data/streaming/kafka_streams/ ./kafka_streams
COPY clients/ ./clients
COPY utils/ ./utils

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/kafka_streams:/clients:/utils:$PYTHONPATH"

# Start Kafka producer and consumer scripts
CMD ["python", "kafka_streams/start_all.py"]