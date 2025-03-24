import datetime
import yfinance as yf
from core.clients.database import Database

db = Database()

try:
    data = db.run_query("SELECT symbol FROM stocks.nyse_stocks;")
    symbols = [row['symbol'] for row in data]
    print(f"Retrieved {len(symbols)} symbols from the database.")
except Exception as e:
    print("Error retrieving symbols from the database:", e)
    symbols = []

# --- Step 2: Define the time period (last 5 years) ---
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=2 * 365)
print(f"Checking historical data from {start_date.date()} to {end_date.date()}.")

# --- Step 3: Check each stock's price change and count matches ---
count = 0
for symbol in symbols:
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
        if hist.empty:
            continue

        first_close = hist['Close'].iloc[0]
        last_close = hist['Close'].iloc[-1]

        # Check if today's closing price is >= 5x the closing price 5 years ago
        if last_close >= 5 * first_close:
            count += 1
            print(f"{symbol}: {first_close:.2f} -> {last_close:.2f}")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

print(f"\nTotal stocks with price >= 3x increase in the last 5 years: {count}")