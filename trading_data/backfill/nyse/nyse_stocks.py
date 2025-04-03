import os
import numpy as np
import requests
import pandas as pd
from clients.database import Database

db = Database()

api_key = os.getenv("FINANCIAL_MODELING_PREP_API_KEY")


def update_nyse_stocks():
    """Retrieve and process the data from Financial Modeling Prep."""
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?exchange=NYSE&limit=10000&apikey={api_key}"

    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    df = df.replace({np.nan: None})
    df = df[['symbol', 'companyName', 'sector', 'industry',
             'beta', 'price', 'lastAnnualDividend', 'volume',
             'country', 'isEtf', 'isFund', 'isActivelyTrading']]
    df.rename(columns={
        'companyName': 'company_name',
        'lastAnnualDividend': 'last_annual_dividend',
        'isEtf': 'is_etf',
        'isFund': 'is_fund',
        'isActivelyTrading': 'is_actively_trading'
    }, inplace=True)

    print(df.head(10).to_string())

    # Updated upsert statement
    insert_sql = """
    INSERT INTO stocks.nyse_stocks (
        symbol, company_name, sector, industry,
        beta, price, last_annual_dividend, volume,
        country, is_etf, is_fund, is_actively_trading
    )
    VALUES %s
    ON CONFLICT (symbol) DO UPDATE SET
        company_name = EXCLUDED.company_name,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        beta = EXCLUDED.beta,
        price = EXCLUDED.price,
        last_annual_dividend = EXCLUDED.last_annual_dividend,
        volume = EXCLUDED.volume,
        country = EXCLUDED.country,
        is_etf = EXCLUDED.is_etf,
        is_fund = EXCLUDED.is_fund,
        is_actively_trading = EXCLUDED.is_actively_trading;
    """
    # Convert DataFrame to list of tuples including the new columns
    data_tuples = list(df[[
        'symbol', 'company_name', 'sector', 'industry',
        'beta', 'price', 'last_annual_dividend', 'volume',
        'country', 'is_etf', 'is_fund', 'is_actively_trading'
    ]].itertuples(index=False, name=None))
    db.run_query(insert_sql, data_tuples)


if __name__ == "__main__":
    update_nyse_stocks()
