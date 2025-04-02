FROM python:3.10-slim

WORKDIR /kafka

# Copy the kafka folder to root dir
COPY trading_data/streaming/kafka/ ./

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Default: run all producer and consumer scripts
CMD ["python", "start_all.py"]