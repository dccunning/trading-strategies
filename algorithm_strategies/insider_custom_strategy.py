import pandas as pd
from datetime import timedelta

# ---- STRATEGY PARAMETERS ----
CAPITAL = 5000                           # Total capital available
TARGET_PROFIT = 4 / 100                  # Sell if 4% profit reached
STOP_LOSS = 2 / 100                      # Sell if 2% loss reached
MIN_POSITION_EUR = 0.01 * CAPITAL        # Only execute trades worth at least 1% of capital
MIN_INSIDER_RATIO = 0.05                 # Insider must have traded at least 5% of their holdings
TRADE_SCALING = 0.1                      # You mirror 10% of the insider's traded volume


def run_strategy(df):
    results = []

    for _, row in df.iterrows():
        P_t = row['price']                         # Insider trade price
        V_i = row['shares_traded']                 # Shares traded by insider
        H = row['insider_holding']                 # Insider's total holdings
        if H == 0 or V_i == 0:
            continue

        # 1. Signal strength = % of insider’s holdings traded
        F = V_i / H
        if F < MIN_INSIDER_RATIO:
            continue  # Skip if trade is too small to matter

        # 2. Max you can buy based on your capital
        N_max = int(CAPITAL // P_t)
        N_buy = min(N_max, int(TRADE_SCALING * V_i))
        if N_buy == 0:
            continue

        # 3. Skip if this trade doesn’t use enough capital (e.g. < 1% of portfolio)
        est_position_value = N_buy * P_t
        if est_position_value < MIN_POSITION_EUR:
            continue

        # 4. Define sell conditions
        profit_target = P_t * (1 + TARGET_PROFIT)
        stop_loss = P_t * (1 - STOP_LOSS)

        # 5. Simulate exit
        if row['future_max_price_30d'] >= profit_target:
            exit_price = profit_target
            exit_reason = 'profit_hit'
        elif row['future_min_price_30d'] <= stop_loss:
            exit_price = stop_loss
            exit_reason = 'stop_loss'
        else:
            exit_price = row['price_30d_later']
            exit_reason = 'time_exit'

        # 6. Calculate profit
        profit = (exit_price - P_t) * N_buy
        roi = profit / est_position_value

        # 7. Record the trade
        results.append({
            'stock': row['stock'],
            'buy_price': P_t,
            'exit_price': exit_price,
            'shares': N_buy,
            'profit': round(profit, 2),
            'roi': round(roi, 4),
            'exit_reason': exit_reason,
            'report_date': row['report_date']
        })

    return pd.DataFrame(results)


def enrich_with_backtest_data(df, price_history):
    df = df.copy()
    df['Filing Date'] = pd.to_datetime(df['Filing Date'])

    results = []

    for _, row in df.iterrows():
        ticker = row['Ticker']
        filing_date = row['Filing Date']
        insider_price = row['Price']

        if ticker not in price_history:
            continue

        hist = price_history[ticker].copy()
        hist['date'] = pd.to_datetime(hist['date'])
        hist = hist[hist['date'] >= filing_date].sort_values('date')

        if hist.empty:
            continue

        entry_row = hist.iloc[0]
        filing_price = entry_row['close']

        target_price = filing_price * 1.04
        stop_price = filing_price * 0.98
        price_30d_later = None
        date_target_hit = None
        date_stop_hit = None

        for _, h_row in hist.iterrows():
            days_since_filing = (h_row['date'] - filing_date).days
            price = h_row['close']

            if date_target_hit is None and price >= target_price:
                date_target_hit = h_row['date']

            if date_stop_hit is None and price <= stop_price:
                date_stop_hit = h_row['date']

            if days_since_filing == 30:
                price_30d_later = price

        results.append({
            'filing_date_price': filing_price,
            'next_date_at_4pct_increase': date_target_hit,
            'next_date_at_2pct_decrease': date_stop_hit,
            'price_30d_later': price_30d_later
        })

    enriched_df = df.iloc[:len(results)].copy()
    extra = pd.DataFrame(results)
    return pd.concat([enriched_df.reset_index(drop=True), extra], axis=1)
