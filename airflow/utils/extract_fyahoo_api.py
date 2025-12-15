import yfinance as yf
import pandas as pd
import logging
import datetime

# Extract Dividend Data from Yahoo Finance API
def extract_dividends_data(symbol):
    """
    Extracts dividend history for a given stock symbol using Yahoo Finance API.

    Parameters:
        symbol (str): Stock ticker symbol (e.g., 'AAPL', 'NVDA')

    Returns:
        pd.DataFrame: DataFrame containing dividend data with metadata
    """

    try:
        ticker = yf.Ticker(symbol)
        dividends = ticker.dividends

        if dividends.empty:
            logging.warning(f"No dividend data found for {symbol}.")
            return pd.DataFrame(columns=["Date", "Dividends", "Symbol", "Ingested_Time"])

        dividends_df = dividends.reset_index()

        print(f"The '{symbol}' dividends data extraction success!")
        logging.info(f"[{symbol}] Dividend data extracted successfully.")
        logging.info(f"Rows: {len(dividends_df)}, Columns: {len(dividends_df.columns)}")
        return dividends_df

    except Exception as e:
        print(f"The ERROR is: {e}")

# Extract S&P 500 Price Data
def extract_sp500():
    # S&P 500 index ticker on Yahoo Finance
    ticker = "^GSPC"
    try:
        sp500 = yf.Ticker(ticker)

        # Historical daily prices
        df = sp500.history(period="max")  # You can use '1y', '5y', 'max', etc.
        df
    except Exception as e:
        print(f"The ERROR is: {e}")

def get_sp500_list():
    # Get the list of S&P 500 companies from a reliable source
    url = "https://www.slickcharts.com/sp500"
    df = pd.read_html(url)[0]
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)  # fix BRK.B → BRK-B
    sp500_list = df['Symbol'].values.tolist()
    print("S&P 500 company list extracted successfully.")
    print(f"Total companies in S&P 500: {len(sp500_list)}")
    return sp500_list

def extract_sp500_profile():
    # S&P 500 index ticker on Yahoo Finance
    try:
        # Get S&P 500 company list
        sp500_list = get_sp500_list()

        # Initialize an empty list to store data
        data_list = []

        for symbol in sp500_list:
            ticker = yf.Ticker(symbol)
            info = ticker.info  # Full metadata

            data_list.append({
                "Exchange": info.get("exchange"),
                "Symbol": symbol,
                "Shortname": info.get("shortName"),
                "Longname": info.get("longName"),
                "Sector": info.get("sector"),
                "Industry": info.get("industry"),
                "Current_Price": info.get("currentPrice"),
                "Market_Cap": info.get("marketCap"),
                "Net_Income_To_Common": info.get("netIncomeToCommon"),
                "Ebitda": info.get("ebitda"),
                "Revenue_Growth": info.get("revenueGrowth"),
            })

            ## Full version with all fields
            # data_list.append({
            #     "Exchange": info.get("exchange"),
            #     "Symbol": symbol,
            #     "Short_Name": info.get("shortName"),
            #     "Long_Name": info.get("longName"),
            #     "Long_Business_Sum":info.get("longBusinessSummary"),
            #     "Sector": info.get("sector"),
            #     "Industry": info.get("industry"),
            #     "Current_Price": info.get("currentPrice"),
            #     "Market_Cap": info.get("marketCap"),
            #     "Ebitda": info.get("ebitda"),
            #     "All_Time_High": info.get("allTimeHigh"),
            #     "All_Time_Low": info.get("allTimeLow"),
            #     "Price_To_Sales_TTM": info.get("priceToSalesTrailing12Months"),
            #     "Fifty_Day_Avg": info.get("fiftyDayAverage"),
            #     "Two_Hundred_Day_Avg": info.get("twoHundredDayAverage"),
            #     "Trailing_Annual_Dividend_Rate": info.get("trailingAnnualDividendRate"),
            #     "Trailing_Annual_Dividend_Yield": info.get("trailingAnnualDividendYield"),
            #     "Currency": info.get("currency"),
            #     "Tradeable": info.get("tradeable"),
            #     "Quote_Type": info.get("quoteType"),
            #     "Enterprise_Value": info.get("enterpriseValue"),
            #     "Profit_Margins": info.get("profitMargins"),
            #     "Float_Shares": info.get("floatShares"),
            #     "Shares_Outstanding": info.get("sharesOutstanding"),
            #     "Shares_Short": info.get("sharesShort"),
            #     "Shares_Short_Prior_Month": info.get("sharesShortPriorMonth"),
            #     "Shares_Short_Prev_Month_Date": info.get("sharesShortPreviousMonthDate"),
            #     "Date_Short_Interest": info.get("dateShortInterest"),
            #     "Shares_Percent_Shares_Out": info.get("sharesPercentSharesOut"),
            #     "Short_Ratio": info.get("shortRatio"),
            #     "Short_Percent_Of_Float": info.get("shortPercentOfFloat"),
            #     "Held_Percent_Insiders": info.get("heldPercentInsiders"),
            #     "Held_Percent_Institutions": info.get("heldPercentInstitutions"),
            #     "Book_Value": info.get("bookValue"),
            #     "Price_To_Book": info.get("priceToBook"),
            #     "Last_Fiscal_Year_End": info.get("lastFiscalYearEnd"),
            #     "Next_Fiscal_Year_End": info.get("nextFiscalYearEnd"),
            #     "Most_Recent_Quarter": info.get("mostRecentQuarter"),
            #     "Earnings_Quarterly_Growth": info.get("earningsQuarterlyGrowth"),
            #     "Net_Income_To_Common": info.get("netIncomeToCommon"),
            #     "Average_Analyst_Rating": info.get("averageAnalystRating"),
            #     "Officers_0_name": info.get("companyOfficers")[0].get("name"),
            #     "Officers_0_title": info.get("companyOfficers")[0].get("title"),
            #     "Officers_0_yearBorn": info.get("companyOfficers")[0].get("yearBorn"),
            #     "Officers_0_fiscalYear": info.get("companyOfficers")[0].get("fiscalYear"),
            #     "Officers_0_totalPay": info.get("companyOfficers")[0].get("totalPay"),
            #     "Officers_1_name": info.get("companyOfficers")[1].get("name"),
            #     "Officers_1_title": info.get("companyOfficers")[1].get("title"),
            #     "Officers_1_yearBorn": info.get("companyOfficers")[1].get("yearBorn"),
            #     "Officers_1_fiscalYear": info.get("companyOfficers")[1].get("fiscalYear"),
            #     "Officers_1_totalPay": info.get("companyOfficers")[1].get("totalPay"),
            #     "Officers_2_name": info.get("companyOfficers")[2].get("name"),
            #     "Officers_2_title": info.get("companyOfficers")[2].get("title"),
            #     "Officers_2_yearBorn": info.get("companyOfficers")[2].get("yearBorn"),
            #     "Officers_2_fiscalYear": info.get("companyOfficers")[2].get("fiscalYear"),
            #     "Officers_2_totalPay": info.get("companyOfficers")[2].get("totalPay"),
            #     "Officers_3_name": info.get("companyOfficers")[3].get("name"),
            #     "Officers_3_title": info.get("companyOfficers")[3].get("title"),
            #     "Officers_3_yearBorn": info.get("companyOfficers")[3].get("yearBorn"),
            #     "Officers_3_fiscalYear": info.get("companyOfficers")[3].get("fiscalYear"),
            #     "Officers_3_totalPay": info.get("companyOfficers")[3].get("totalPay"),
            #     "Officers_4_name": info.get("companyOfficers")[4].get("name"),
            #     "Officers_4_title": info.get("companyOfficers")[4].get("title"),
            #     "Officers_4_yearBorn": info.get("companyOfficers")[4].get("yearBorn"),
            #     "Officers_4_fiscalYear": info.get("companyOfficers")[4].get("fiscalYear"),
            #     "Officers_4_totalPay": info.get("companyOfficers")[4].get("totalPay"),
            #     "Officers_5_name": info.get("companyOfficers")[5].get("name"),
            #     "Officers_5_title": info.get("companyOfficers")[5].get("title"),
            #     "Officers_5_yearBorn": info.get("companyOfficers")[5].get("yearBorn"),
            #     "Officers_5_fiscalYear": info.get("companyOfficers")[5].get("fiscalYear"),
            #     "Officers_5_totalPay": info.get("companyOfficers")[5].get("totalPay"),
            #     "Officers_6_name": info.get("companyOfficers")[6].get("name"),
            #     "Officers_6_title": info.get("companyOfficers")[6].get("title"),
            #     "Officers_6_yearBorn": info.get("companyOfficers")[6].get("yearBorn"),
            #     "Officers_6_fiscalYear": info.get("companyOfficers")[6].get("fiscalYear"),
            #     "Officers_6_totalPay": info.get("companyOfficers")[6].get("totalPay"),
            #     "Officers_7_name": info.get("companyOfficers")[7].get("name"),
            #     "Officers_7_title": info.get("companyOfficers")[7].get("title"),
            #     "Officers_7_yearBorn": info.get("companyOfficers")[7].get("yearBorn"),
            #     "Officers_7_fiscalYear": info.get("companyOfficers")[7].get("fiscalYear"),
            #     "Officers_7_totalPay": info.get("companyOfficers")[7].get("totalPay"),
            #     "Officers_8_name": info.get("companyOfficers")[8].get("name"),
            #     "Officers_8_title": info.get("companyOfficers")[8].get("title"),
            #     "Officers_8_yearBorn": info.get("companyOfficers")[8].get("yearBorn"),
            #     "Officers_8_fiscalYear": info.get("companyOfficers")[8].get("fiscalYear"),
            #     "Officers_8_totalPay": info.get("companyOfficers")[8].get("totalPay"),
            #     "Officers_9_name": info.get("companyOfficers")[9].get("name"),
            #     "Officers_9_title": info.get("companyOfficers")[9].get("title"),
            #     "Officers_9_yearBorn": info.get("companyOfficers")[9].get("yearBorn"),
            #     "Officers_9_fiscalYear": info.get("companyOfficers")[9].get("fiscalYear"),
            #     "Officers_9_totalPay": info.get("companyOfficers")[9].get("totalPay"),
            # })

        sp_profile_df = pd.DataFrame(data_list)
        return sp_profile_df
    except Exception as e:
        print(f"The ERROR is: {e}")


def extract_sp500_price_history():
    # S&P 500 index ticker on Yahoo Finance
    try:
        # Get S&P 500 company list
        sp500_list = get_sp500_list()

        # Initialize an empty list to store data
        data_list = []

        for symbol in sp500_list:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period='1mo', interval='1d', start=None, end=None)  # Historical price data

            hist.reset_index(inplace=True)
            hist['Symbol'] = symbol  # Add symbol column

            data_list.append(hist)

        sp_price_history_df = pd.concat(data_list, ignore_index=True)
        return sp_price_history_df
    except Exception as e:
        print(f"The ERROR is: {e}")