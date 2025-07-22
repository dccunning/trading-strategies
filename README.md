### Introduction
#### Trading Strategy Playground

This is a web-based platform for backtesting trading strategies. Users can define strategy parameters and run tests on historical stock price data across selected stocks and timeframes.

Currently implemented strategies include:
1. Momentum - Buys stocks breaking out to new highs and strong volume (medium term)
2. Move on Strength - Buys stocks with accelerating trends (long term)
3. Mean Reversion - Targets intraday price dips below an anchored VWAP (short term)
4. Net Asset Value - Buys undervalued stocks where the market price below NCAV (long term)


### Tech Stack
1. PostgreSQL - Stores all stock price data for backtesting
2. Python backend - Includes Kafka streams, Data retrival clients, strategy logic and backtest engine
3. Streamlit frontend - UI for configuring and running backtests, and viewing results


[//]: # (### Usage)

[//]: # ()
[//]: # (Examples of parameters and output here)

[//]: # ()
[//]: # ()
[//]: # (### Add a Strategy)

[//]: # ()
[//]: # (How to add a new strategy instructions here)

[//]: # ()
[//]: # ()
[//]: # (### Database schema)

[//]: # ()
[//]: # (Define tables here)
