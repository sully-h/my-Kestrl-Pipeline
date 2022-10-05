import pandas as pd
import numpy as np
import yahooquery

def get_LSE_symbols_data():
    LSE_symbols = pd.read_table("data-sources/LSE.txt", index_col="Description")  # from EOD, 5000 rows including non-equity
    #my_stocks = LSE_symbols.copy()
    return LSE_symbols.copy()



def _get_total_revenue_yahoo_query(row):
    '''gets revenue using a row containing a Symbol (stock ticker), returns np.nan for unsuccessful API calls'''
    try:
        return _get_total_revenue_yahoo_query_from_ticker(row["Symbol"] + ".L") # always .L for the LSE
    except Exception as e:  # likewise KeyError that presents as type Error due to TypeError: string indices must be integers
        return np.nan


def _get_total_revenue_yahoo_query_from_ticker(ticker, date='2021'):
    '''gets revenue using a stock ticker from yahoo_query'''
    return \
        yahooquery.Ticker(ticker).income_statement()[["asOfDate", "TotalRevenue"]].dropna().set_index("asOfDate").loc[
            date, "TotalRevenue"].values[0]


if __name__ == '__main__':
    #main_revenue_getter(master_stocks=get_LSE_symbols_data(), path=None, save_to_file=None)
    pass