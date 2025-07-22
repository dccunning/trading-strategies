from pandas import DataFrame
from secfsdstools.e_collector.companycollecting import CompanyReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, OfficialTagsOnlyRawFilter, \
    USDOnlyRawFilter, ReportPeriodAndPreviousPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer
import requests
import pandas as pd

MILLIONS = 1_000_000
CashFlowColumns = ['adsh', 'cik', 'name', 'form', 'fye', 'fy', 'fp', 'date', 'filed', 'coreg', 'report', 'ddate',
                   'qtrs',
                   'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
                   'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
                   'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations',
                   'NetCashProvidedByUsedInOperatingActivities',
                   'NetCashProvidedByUsedInFinancingActivities', 'NetCashProvidedByUsedInInvestingActivities',
                   'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
                   'CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations',
                   'CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations', 'EffectOfExchangeRateFinal',
                   'CashPeriodIncreaseDecreaseIncludingExRateEffectFinal', 'CashAndCashEquivalentsEndOfPeriod',
                   'DepreciationDepletionAndAmortization',
                   'DeferredIncomeTaxExpenseBenefit', 'ShareBasedCompensation', 'IncreaseDecreaseInAccountsPayable',
                   'IncreaseDecreaseInAccruedLiabilities',
                   'InterestPaidNet', 'IncomeTaxesPaidNet', 'PaymentsToAcquirePropertyPlantAndEquipment',
                   'ProceedsFromSaleOfPropertyPlantAndEquipment',
                   'PaymentsToAcquireInvestments', 'ProceedsFromSaleOfInvestments',
                   'PaymentsToAcquireBusinessesNetOfCashAcquired',
                   'ProceedsFromDivestitureOfBusinessesNetOfCashDivested', 'PaymentsToAcquireIntangibleAssets',
                   'ProceedsFromSaleOfIntangibleAssets',
                   'ProceedsFromIssuanceOfCommonStock', 'ProceedsFromStockOptionsExercised',
                   'PaymentsForRepurchaseOfCommonStock',
                   'ProceedsFromIssuanceOfDebt', 'RepaymentsOfDebt', 'PaymentsOfDividends', 'BaseOpAct_error',
                   'BaseOpAct_cat', 'BaseFinAct_error',
                   'BaseFinAct_cat', 'BaseInvAct_error', 'BaseInvAct_cat', 'NetCashContOp_error', 'NetCashContOp_cat',
                   'CashEoP_error', 'CashEoP_cat']
BalanceSheetColumns = ['adsh', 'cik', 'name', 'form', 'fye', 'fy', 'fp', 'date', 'filed', 'coreg', 'report', 'ddate',
                       'qtrs', 'Assets', 'AssetsCurrent', 'Cash',
                       'AssetsNoncurrent', 'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent', 'Equity',
                       'HolderEquity', 'RetainedEarnings',
                       'AdditionalPaidInCapital', 'TreasuryStockValue', 'TemporaryEquity', 'RedeemableEquity',
                       'LiabilitiesAndEquity', 'AssetsCheck_error',
                       'AssetsCheck_cat', 'LiabilitiesCheck_error', 'LiabilitiesCheck_cat', 'EquityCheck_error',
                       'EquityCheck_cat', 'AssetsLiaEquCheck_error',
                       'AssetsLiaEquCheck_cat']
IncomeStatementColumns = [
    'adsh', 'cik', 'name', 'form', 'fye', 'fy', 'fp', 'date', 'filed', 'coreg', 'report', 'ddate', 'qtrs', 'Revenues',
    'CostOfRevenue',
    'GrossProfit', 'OperatingExpenses', 'OperatingIncomeLoss',
    'IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
    'AllIncomeTaxExpenseBenefit', 'IncomeLossFromContinuingOperations', 'IncomeLossFromDiscontinuedOperationsNetOfTax',
    'ProfitLoss',
    'NetIncomeLossAttributableToNoncontrollingInterest', 'NetIncomeLoss', 'OutstandingShares', 'EarningsPerShare',
    'RevCogGrossCheck_error',
    'RevCogGrossCheck_cat', 'GrossOpexpOpil_error', 'GrossOpexpOpil_cat', 'ContIncTax_error', 'ContIncTax_cat',
    'ProfitLoss_error',
    'ProfitLoss_cat', 'NetIncomeLoss_error', 'NetIncomeLoss_cat', 'EPS_error', 'EPS_cat']


def fetch_financial_data_sec(cik: int):
    collector = CompanyReportCollector.get_company_collector(ciks=[cik])

    raw_data_bag = collector.collect()
    filtered_bag = raw_data_bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][
        USDOnlyRawFilter()]
    joined_bag = filtered_bag.join()

    cf_standardizer = CashFlowStandardizer()
    bs_standardizer = BalanceSheetStandardizer()
    is_standardizer = IncomeStatementStandardizer()

    cf_df = joined_bag.present(cf_standardizer)
    cf_df = cf_df[(cf_df.fp == "FY") & (cf_df.qtrs == 4) & (cf_df.form == "10-K")].copy()

    bs_df = joined_bag.present(bs_standardizer)
    bs_df = bs_df[(bs_df.fp == "FY") & (bs_df.form == "10-K")].copy()

    is_df = joined_bag.present(is_standardizer)
    is_df = is_df[(is_df.fp == "FY") & (is_df.qtrs == 4) & (is_df.form == "10-K")].copy()

    # Get the common years across all dataframes
    common_years = set(cf_df['fy']) & set(bs_df['fy']) & set(is_df['fy'])

    # Filter dataframes to include only common years
    cf_df = cf_df[cf_df['fy'].isin(common_years)]
    bs_df = bs_df[bs_df['fy'].isin(common_years)]
    is_df = is_df[is_df['fy'].isin(common_years)]

    # Sort dataframes by 'fy' in descending order (most recent year first)
    cf_df = cf_df.sort_values('fy', ascending=False).reset_index(drop=True)
    bs_df = bs_df.sort_values('fy', ascending=False).reset_index(drop=True)
    is_df = is_df.sort_values('fy', ascending=False).reset_index(drop=True)

    # Ensure all dataframes have the same number of rows
    min_rows = min(len(cf_df), len(bs_df), len(is_df))
    cf_df = cf_df.head(min_rows)
    bs_df = bs_df.head(min_rows)
    is_df = is_df.head(min_rows)

    # Final check to ensure 'fy' values match across all dataframes
    if not (cf_df['fy'].equals(bs_df['fy']) and cf_df['fy'].equals(is_df['fy'])):
        raise ValueError("Mismatch in fiscal years across financial statements")

    return cf_df, bs_df, is_df


def stock_yoy_breakdown(stock_data: DataFrame):
    merged = stock_data
    # grouped = merged.groupby(['ddate', 'Assets']).size().reset_index(name='Count')

    merged['Year'] = merged['ddate'].astype(str).str[:4].astype('Int64')
    merged['PeriodEnd'] = pd.to_datetime(merged['date'], errors='coerce').dt.strftime('%m-%d')
    annual = merged[merged['fp'] == 'FY']
    annual = annual.drop_duplicates(subset=['Year']).sort_values('Year')

    annual['Revenues '] = (annual['Revenues'] / MILLIONS).round(0).astype('Int64')
    annual['Assets '] = (annual['Assets'] / MILLIONS).round(0).astype('Int64')
    annual['Revs Avg3'] = annual['Revenues '].rolling(window=3, min_periods=3).mean().round(0).astype('Int64')
    annual['Outstanding Sh '] = (annual['OutstandingShares'] / MILLIONS).round(0).astype('Int64')

    annual['AssetTurnover'] = (annual['Revenues'] / annual['Assets'])
    annual['Turnover Avg3'] = annual['AssetTurnover'].rolling(window=3, min_periods=3).mean().round(2)

    annual['Gross Profit / TOA'] = annual['GrossProfit'] / (annual['Assets'] - (
            annual['LiabilitiesCurrent'] + annual['LiabilitiesNoncurrent'] - (
            annual['ProceedsFromIssuanceOfDebt'].fillna(0) - annual['RepaymentsOfDebt'].fillna(0))).replace(0,
                                                                                                            pd.NA))
    annual['Gross Profit / TOA Avg3'] = annual['Gross Profit / TOA'].rolling(window=3, min_periods=3).mean().round(2)

    annual['EBIT_adj / TOA'] = (
            (annual['OperatingIncomeLoss'] + annual['DepreciationDepletionAndAmortization'].fillna(0)) / (
            annual['Assets'] - (annual['LiabilitiesCurrent'] + annual['LiabilitiesNoncurrent'] - (
            annual['ProceedsFromIssuanceOfDebt'].fillna(0) - annual['RepaymentsOfDebt'].fillna(0))).replace(
        0, pd.NA)))
    annual['EBIT adj / TOA Avg3'] = annual['EBIT_adj / TOA'].rolling(window=3, min_periods=3).mean().round(2)

    annual['ROIC'] = (annual['IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit'] - annual[
        'AllIncomeTaxExpenseBenefit']) / (annual['Assets'] - (
            annual['LiabilitiesCurrent'] + annual['LiabilitiesNoncurrent'] - (
            annual['ProceedsFromIssuanceOfDebt'].fillna(0) - annual['RepaymentsOfDebt'].fillna(0))))
    annual['ROIC Avg3'] = annual['ROIC'].rolling(window=3, min_periods=3).mean().round(2)

    annual['CROSIC'] = annual['NetCashProvidedByUsedInOperatingActivities'] / (annual['Assets'] - (
            annual['LiabilitiesCurrent'] + annual['LiabilitiesNoncurrent'] - (
            annual['ProceedsFromIssuanceOfDebt'].fillna(0) - annual['RepaymentsOfDebt'].fillna(0))))
    annual['CROSIC Avg3'] = annual['CROSIC'].rolling(window=3, min_periods=3).mean().round(2)

    annual['GrossMargin'] = annual['GrossProfit'] / annual['Revenues']
    annual['GrossMargin Avg3'] = annual['GrossMargin'].rolling(window=3, min_periods=3).mean().round(2)

    annual['EBITDA Margin Avg3'] = (
            (annual['OperatingIncomeLoss'] + annual['DepreciationDepletionAndAmortization'].fillna(0)) / annual[
        'Revenues'].replace(0, pd.NA)).rolling(window=3, min_periods=3).mean().round(2)

    annual['Net Inc Margin Avg3'] = (annual['NetIncomeLoss'].fillna(0) / annual['Revenues'].replace(0, pd.NA)).rolling(
        window=3, min_periods=3).mean().round(2)

    annual['CFO Margin Avg3'] = (
            annual['NetCashProvidedByUsedInOperatingActivities'].fillna(0) / annual['Revenues'].replace(0,
                                                                                                        pd.NA)).rolling(
        window=3, min_periods=3).mean().round(2)

    annual['SFCF Margin Avg3'] = ((annual['NetCashProvidedByUsedInOperatingActivities'] - annual[
        'PaymentsToAcquirePropertyPlantAndEquipment']) / annual['Revenues'].replace(0, pd.NA)).rolling(window=3,
                                                                                                       min_periods=3).mean().round(
        2)

    annual['NCF Margin Avg3'] = (
            annual['CashPeriodIncreaseDecreaseIncludingExRateEffectFinal'].fillna(0) / annual['Revenues'].replace(0,
                                                                                                                  pd.NA)).rolling(
        window=3, min_periods=3).mean().round(2)

    annual['Revs/sh'] = (annual['Revenues'].fillna(0) / annual['OutstandingShares'].replace(0, pd.NA)).round(2)

    annual['Assets/sh'] = (annual['Assets'].fillna(0) / annual['OutstandingShares'].replace(0, pd.NA)).round(2)

    annual['Book/sh'] = (annual['Equity'].fillna(0) / annual['OutstandingShares'].replace(0, pd.NA)).round(2)

    annual['Net Excess Cash/sh'] = (
            annual['CashAndCashEquivalentsEndOfPeriod'].fillna(0) / annual['OutstandingShares'].replace(0,
                                                                                                        pd.NA)).round(
        2)

    annual['Div/sh'] = (annual['PaymentsOfDividends'].fillna(0) / annual['OutstandingShares'].replace(0, pd.NA)).round(
        2)

    annual['ROE 3yr%'] = (annual['NetIncomeLoss'].fillna(0) / annual['Equity'].replace(0, pd.NA)).rolling(window=3,
                                                                                                          min_periods=3).mean().round(
        2)
    annual['ROE 5yr%'] = (annual['NetIncomeLoss'].fillna(0) / annual['Equity'].replace(0, pd.NA)).rolling(window=5,
                                                                                                          min_periods=5).mean().round(
        2)

    annual = annual[[
        'Year',  # Year reported
        'PeriodEnd',
        'Outstanding Sh ',
        'Revenues ',
        'Revs Avg3',
        'Turnover Avg3',  # Revenues  / Assets, 3y MA
        'Gross Profit / TOA Avg3',  # How efficiently a company generates gross profit from its operating assets
        'EBIT adj / TOA Avg3',
        # How effectively a company generates core operating earnings from its Total Operating Assets
        'ROIC Avg3',
        # Return on Invested Capital — measures how effectively a company uses all capital invested in its core operations to generate after-tax operating profit
        'CROSIC Avg3',  # Cash Return on Operating/Invested Capital - focuses on cash flow instead of accounting profits
        'GrossMargin Avg3',  # How efficiently a company is producing and selling its goods or services
        'EBITDA Margin Avg3',  # How much revenue turns into operating cash flow before interest, taxes, etc.
        'Net Inc Margin Avg3',  # The percentage of revenue that remains as net profit after all expenses are paid
        'CFO Margin Avg3',  # The ability to convert its revenue into operating cash
        'SFCF Margin Avg3',
        # Stable Free Cash Flow - How much free cash flow available to equity holders is generated as a percentage of revenue
        'NCF Margin Avg3',  # Net Cash Flow Margin - The percentage of a company’s revenue that results in net cash flow
        'Revs/sh',  # Revenue per share
        'Assets/sh',  # Assets per share
        'Book/sh',  # Equity per share
        'Net Excess Cash/sh',  # Available cash per share
        'Div/sh',  # Yearly dividend paid in $ per share
        'ROE 3yr%',  # Return on equity 3y MA
        'ROE 5yr%',  # Return on equity 5y MA
    ]]

    return annual.set_index(['Year', 'PeriodEnd']).transpose()


def get_ticker_mapping():
    """
    Return all tickers in the format:

    {'EXCHANGE:SYMBOL': cik, ... }
    """

    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    headers = {
        "User-Agent": "MyAppName support@mydomain.com",
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json",
        "Host": "www.sec.gov"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return {
        f"{item[3].upper() if item[3] else 'None'}:{item[2]}": item[0] for item in data['data']
    }


def save_ticker_data(exchange_ticker: str, cik: int):
    """
    Save SEC data for one ticker, given cik number and ticker string: EXCHANGE:TICKER
    """
    cf_df, bs_df, is_df = fetch_financial_data_sec(cik=cik)
    merged = (
        cf_df
        .merge(bs_df, on=["cik", "name", "form", "fye", "ddate"], how='outer')
        .merge(is_df, on=["cik", "name", "form", "fye", "ddate"], how='outer')
    )
    exchange_ticker_list = exchange_ticker.split(':')
    merged.to_csv(f"sec_stock_data/{exchange_ticker_list[0]}__{exchange_ticker_list[1]}.csv", index_label=False)


def save_from_list(tickers: list):
    cik_mapping = get_ticker_mapping()
    for t in tickers:
        try:
            save_ticker_data(exchange_ticker=t, cik=cik_mapping.get(t))
            print('Done:', t)
        except Exception as e:
            print('Failed:', t, str(e))


# if __name__ == "__main__":
    # save_from_list(['NYSE:SBSW'])
    # data = pd.read_csv("sec_stock_data/LULU.csv")
    # print(stock_yoy_breakdown(data).index)
