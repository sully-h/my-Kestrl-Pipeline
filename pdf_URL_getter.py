import pickle
import json

import requests
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

from urllib.parse import urlencode, urlunparse
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

from secrets import CX, CUSTOM_SEARCH_API_KEY

# these just for testing purposes
trouble_some_pdfs = {}
master_stocks = pd.read_csv("EOD_LSE_filtered_master_data.csv")
master_stocks = master_stocks[:20]

def get_URL_bs4(row):
    #_response = requests.get(,headers=headers)
    pass

def get_URL_bing_scrape(row, trouble_some_pdfs):
    URL= None
    _params = {}
    headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    try:
        # TODO scrape Bing for filetype:pdf company name annual report
        # https://stackoverflow.com/questions/61226395/get-bing-search-results-in-python
        query = f"filetype:pdf {row['Description']} annual report"
        url = urllib.parse.urlunparse(("https", "www.bing.com", "/search", "", urllib.parse.urlencode({"q": query}), ""))
        #custom_user_agent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"
        req = urllib.request.Request(url, headers=headers)
        page = urllib.request.urlopen(req)
        # Further code I've left unmodified
        soup = BeautifulSoup(page.read())
        links = soup.findAll("a")
        top_URL = links[0]["href"]
        return top_URL
        #for link in links:
         #   print(link["href"])
    except:
        trouble_some_pdfs[row["Symbol"]] = np.nan

        with open("tests/troublesome_URLs.json", 'w') as f:
            json.dump(trouble_some_pdfs, f, indent=2)

        return np.nan

def get_URL_bing(row, trouble_some_pdfs):
    URL= None
    _params = {}
    headers = {}

    try:
        # TODO scrape Bing for filetype:pdf company name annual report
        return
    except:
        trouble_some_pdfs[row["Symbol"]] = np.nan

        with open("tests/troublesome_URLs.json", 'w') as f:
            json.dump(trouble_some_pdfs, f, indent=2)

        return np.nan

def get_URL_google(row, trouble_some_pdfs):
    '''goes through each row of dataset and searches for first URL link with filetype:pdf <Company Name> annual report'''
    #URL = "http://www.google.com/search?"
    URL = "https://customsearch.googleapis.com/customsearch/v1?"
    _params = {
        'client': 'google-csbe',
        'output': 'xml',
        'cx': CX,
        'q': f'filetype:pdf {row["Description"]} annual report',
        #'filetype': 'pdf',
        'start': '1',
        'num': '2',
        'key': CUSTOM_SEARCH_API_KEY
    }

    headers = {'Accept': 'application/json',
               "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36',
               }  # this necessary? idk

    try:
        _response = requests.get(URL, _params, headers=headers)
        return _response.json()['items'][0]['link']  # the URL of the first link
    except:  # ORGASYNTH EUR2 annual report filetype:pdf doesn't exist on the web!!
        trouble_some_pdfs[row["Symbol"]] = np.nan

        with open("tests/troublesome_URLs.json", 'w') as f:
            json.dump(trouble_some_pdfs, f, indent=2)

        return np.nan


def download_annual_report(row, trouble_some_pdfs: dict, path=None):
    path = f"annual-reports/{row['Symbol']}_annual_report.pdf"

    try:
        response = requests.get(row["annual_report_URL"], stream=True)
        with open(path, 'wb') as f:
            f.write(response.content) # I think I established this was the best way but need to check AGAIN
        return path
    except:
        trouble_some_pdfs[row["Symbol"]] = row["annual_report_URL"]
        with open("tests/troublesome_URLs.json", 'w') as f:
            json.dump(trouble_some_pdfs, f, indent=2)

        return np.nan


def main():
    '''scrapes for link AND downloads.. that's TWO different functions!!'''
    # TODO this function should assume that the data has everything that's needed since it only runs in an except block
    master_stocks = pd.read_csv("my_stocks_master.csv")  # contains revenue and symbols from EOD
    master_stocks = pd.read_csv("EOD_LSE_filtered_master_data.csv")  # 869 rows after
    test_stocks = master_stocks.copy()
    test_stocks = test_stocks[:10]

    test_stocks = test_stocks.dropna()
    master_stocks = master_stocks.dropna()  # idk why this didn't drop all the NaN columns the first time...!

    # I only get 100 requests with google Custom search JSON API

    master_stocks = master_stocks.head(100)



    params =  {}
    trouble_some_pdfs = {}

    master_stocks["annual_report_URL"] = master_stocks.apply(lambda row: get_URL_google(row, trouble_some_pdfs), axis=1) # I commented this out to protect my 100 limit
    master_stocks.to_csv("my_stocks_master_with_URLs_and_PDF_paths.csv")
    master_stocks.to_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv") # although at this point you DON'T have the PDF paths!

    master_stocks = master_stocks.dropna()  # some URLs weren't retrievable - get rid of them

    # uses URL to download PDF and saves the unsuccessful ones to trouble_some_pdfs
    master_stocks["PDF_path"] = master_stocks.apply(lambda row: download_annual_report(row, trouble_some_pdfs),
                                                    axis=1)  # save the pdf and note the path to it

    # FYI some PDFs weren't retrievable from the URLs but I've just kept those rows in the file

    # master_stocks.to_csv("my_stocks_master_with_URLs_and_PDF_paths.csv")
    master_stocks.to_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv")

    # I've been rate limited for today ... :(
    # test_stocks["annual_report_URL"] = test_stocks.apply(lambda row: get_URL_google(row), axis=1)
    # test_stocks = test_stocks.dropna()
    #
    # #test_stocks["PDF_path"] = master_stocks.apply(lambda row: download_annual_report(row), axis=1)
    #
    # test_stocks.to_csv("test_stocks_with_URL_and_PDF_paths.csv")

    with open("tests/troublesome_URLs.json", 'w') as f:
        json.dump(trouble_some_pdfs, f, indent=2)

if __name__ == '__main__':
    main()
    pass
