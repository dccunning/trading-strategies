__name__ = "Dimitri Cunning"

import os
import json
import psycopg2
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'finnhub-nyse-stock-prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

conn = psycopg2.connect(
    dbname=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_EXTERNAL_USER"),
    password=os.getenv("DATABASE_EXTERNAL_USER_PASSWORD"),
    host=os.getenv("DATABASE_HOST"),
    port=os.getenv("DATABASE_PORT")
)
cur = conn.cursor()

for message in consumer:    
    stock_data = message.value
    symbol = stock_data['symbol']
    category = stock_data['category']
    price = stock_data['price']
    timestamp = stock_data['time']

    cur.execute("""
        INSERT INTO stocks.finnhub_data (symbol, category, price, time)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol, time) DO NOTHING;
    """, (symbol, category, price, timestamp))

    conn.commit()

cur.close()
conn.close()

