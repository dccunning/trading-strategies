import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from clients.binance_client import get_all_ohlcv
from common.strategy.performance import Performance

start_dt = datetime.utcnow() - timedelta(days=500)
end_dt = datetime.utcnow()

df = get_all_ohlcv("BTCUSDT", interval="1h", start=start_dt, end=end_dt)
# def crossing_moving_averages():
fast_ma = df['close'].rolling(10).mean()
slow_ma = df['close'].rolling(40).mean()

df['fast_ma'] = fast_ma
df['slow_ma'] = slow_ma

df['signal'] = np.where(fast_ma > slow_ma, 1, 0)

print("Performance")
strategy_performance = Performance(df=df, periods_in_year=1 * 24 * 365, trade_size=1000, capital=1000)
print(strategy_performance.evaluate())
strategy_performance.plot_cumulative_pnl()

