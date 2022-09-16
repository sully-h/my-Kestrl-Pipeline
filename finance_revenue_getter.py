import pandas as pd
import numpy as np
import yahooquery

def get_LSE_symbols_data():
    LSE_symbols = pd.read_table("data-sources/LSE.txt", index_col="Description")  # from EOD, 5000 rows including non-equity
    #my_stocks = LSE_symbols.copy()
    return LSE_symbols.copy()

def main_revenue_getter(stocks, path, save_to_file=True):
    '''adds a column containing Revenue and saves the data to path'''
    stocks["Total Revenue 20/21"] = stocks.apply(lambda row: get_total_revenue_yahoo_query(row), axis=1)
    stocks = stocks.dropna()  # removes the non-equity stock symbols # TODO why are there NaN rows for Company Description remaining??
    if save_to_file:
        stocks.to_csv(path)
    return stocks


def _get_total_revenue_yahoo_query(ticker, date='2021'):
    '''gets revenue using a stock ticker from yahoo_query'''
    return \
        yahooquery.Ticker(ticker).income_statement()[["asOfDate", "TotalRevenue"]].dropna().set_index("asOfDate").loc[
            date, "TotalRevenue"].values[0]


def get_total_revenue_yahoo_query(row):
    '''gets revenue using a row containing a Symbol (stock ticker), catches Exceptions and returns np.nan for unsuccessful API calls'''
    try:
        return _get_total_revenue_yahoo_query(row["Symbol"]) # is there a risk of using the wrong exchange by omitting the .L?
    except:  # Probably KeyError but it will be TypeError due to TypeError: string indices must be integers
        try:
            return _get_total_revenue_yahoo_query(row["Symbol"] + ".L")
        except:  # likewise KeyError that presents as type Error due to TypeError: string indices must be integers
            return np.nan



if __name__ == '__main__':
    main_revenue_getter(stocks=get_LSE_symbols_data(), path=None, save_to_file=None)
