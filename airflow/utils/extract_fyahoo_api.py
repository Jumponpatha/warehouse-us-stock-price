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