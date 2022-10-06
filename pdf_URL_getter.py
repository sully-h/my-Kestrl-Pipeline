import pickle
import json
import time

from pathlib import Path
import scrapy
from datetime import datetime

import requests
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

from urllib.parse import urlencode, urlunparse
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

from secrets import CX, CUSTOM_SEARCH_API_KEY, SCRAPER_KEY

# useful info?: https://www.scraperapi.com/blog/scrape-data-google-search-results/
class SearchScrape:
    def __init__(self, row, trouble_some_pdfs):
        self.row = row
        self.url = None

        self.trouble_some_pdfs = trouble_some_pdfs # this is only to keep track of problem PDFs
        self.html_search_soup = None

        self.SEARCH_TERM = f"http://www.google.com/search?q=filetype%3Apdf+{urllib.parse.quote_plus(self.row['Description'])}+annual+report+2021"  # could add ?asfiletype=pdf
        self.ASYNC_URL = "https://async.scraperapi.com"

        self.r = requests.post(url='https://async.scraperapi.com/jobs', json={'apiKey': SCRAPER_KEY, 'url': self.SEARCH_TERM, "apiparams": {"autoparse": True}})
        self.response = json.loads(self.r.content) # I want to keep the original bytes 'r' <Response> obj just in case..
        self.async_url = self.response["statusUrl"]

        print(f'working on Scraping {self.row["Symbol"]}')

        # you may have to wait around a minute to GET the scraped info
        try:
            self.html_search_soup = self.async_scraper_get()
        except KeyError:
            for _ in range(20):
                time.sleep(5)
                self.html_search_soup = self.async_scraper_get()
                if self.html_search_soup:
                    break
        except Exception as e:
             self.e = e
             self.trouble_some_pdfs[self.row['Symbol']] = e
             self.url = np.nan # this could be raise ValueError and then the other method could take over?

        # once you have the body, parse it
        self.url = self.parse_html_soup() # TODO this needs to be a .pdf
        try:
            if Path(self.url).suffix != ".pdf":
                raise ValueError(f"this URL does not lead to a .pdf? {self.url}")
        except:
            print(f'Secured the URL: {self.url}')

    def async_scraper_get(self):
        self.response_get = requests.get(url=self.async_url)
        self.content_get = self.response_get.content
        self.content_get = json.loads(self.content_get)
        return self.content_get['response']['body'] # this should be the body

    def parse_html_soup(self): # is there a organic_results key in response that easily gives the search link?
        '''parse the html soup for the first search link''' # what if its an ad??
        self.soup = BeautifulSoup(self.html_search_soup, 'html.parser')
        self.first_tag = self.soup.select('.yuRUbf a')[0]
        self.url = self.first_tag.attrs['href']
        #self.soup_divs_lst = self.soup.find_all("div", class_="YuRUbf")[1:]
        #self.url = self.soup_divs_lst[0].find('a')['href'].replace("/url?q=", "") # this line only useful for debugging
        return self.url

    @classmethod
    def from_row(cls):
        pass


def _get_URL_google(row, trouble_some_pdfs):
    '''goes through each row of dataset and searches for first ASYNC_URL link with filetype:pdf <Company Name> annual report 2021'''
    # ASYNC_URL = "http://www.google.com/search?"
    URL = "https://customsearch.googleapis.com/customsearch/v1?"
    _params = {
        'client': 'google-csbe',
        'output': 'xml',
        'cx': CX,
        'q': f'filetype:pdf {row["Description"]} annual report 2021',
        # 'filetype': 'pdf',
        'start': '1',
        'num': '2',
        'key': CUSTOM_SEARCH_API_KEY
    }

    headers = {'Accept': 'application/json',
               "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36',
               }  # this necessary? idk

    try:
        _response = requests.get(URL, _params, headers=headers)
        return _response.json()['items'][0]['link']  # the ASYNC_URL of the first link
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
            f.write(response.content)  # I think I established this was the best way but need to check AGAIN
        return path
    except:
        trouble_some_pdfs[row["Symbol"]] = row["annual_report_URL"]
        with open("tests/troublesome_URLs.json", 'w') as f:
            json.dump(trouble_some_pdfs, f, indent=2)

        return np.nan

# these below 3 funcs for Bing amount to nothing and can go in the scrap
def get_URL_bs4(row):
    # _response = requests.get(,headers=headers)
    pass


def get_URL_bing_scrape(row, trouble_some_pdfs):
    URL = None
    _params = {}
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

    try:
        # TODO scrape Bing for filetype:pdf company name annual report
        # https://stackoverflow.com/questions/61226395/get-bing-search-results-in-python
        query = f"filetype:pdf {row['Description']} annual report"
        url = urllib.parse.urlunparse(
            ("https", "www.bing.com", "/search", "", urllib.parse.urlencode({"q": query}), ""))
        # custom_user_agent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"
        req = urllib.request.Request(url, headers=headers)
        page = urllib.request.urlopen(req)
        # Further code I've left unmodified
        soup = BeautifulSoup(page.read())
        links = soup.findAll("a")
        top_URL = links[0]["href"]
        return top_URL
        # for link in links:
        #   print(link["href"])
    except:
        trouble_some_pdfs[row["Symbol"]] = np.nan

        with open("tests/troublesome_URLs.json", 'w') as f:
            json.dump(trouble_some_pdfs, f, indent=2)

        return np.nan


def get_URL_bing(row, trouble_some_pdfs):
    URL = None
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





def main():
    trouble_some_pdfs = {}
    return pd.read_csv("intermediate data sources/EOD_LSE_filtered_master_data.csv").head(10)
    # symbols_only_data["annual_report_URL"] = symbols_only_data.apply(lambda row: _get_URL_scraper_api(row, trouble_some_pdfs),axis=1)


def main_first():
    '''scrapes for link AND downloads.. that's TWO different functions!!'''
    # TODO this function should assume that the data has everything that's needed since it only runs in an except block
    master_stocks = pd.read_csv("tests/my_stocks_master.csv")  # contains revenue and symbols from EOD
    master_stocks = pd.read_csv("intermediate data sources/EOD_LSE_filtered_master_data.csv")  # 869 rows after
    test_stocks = master_stocks.copy()
    test_stocks = test_stocks[:10]

    test_stocks = test_stocks.dropna()
    master_stocks = master_stocks.dropna()  # idk why this didn't drop all the NaN columns the first time...!

    # I only get 100 requests with google Custom search JSON API

    master_stocks = master_stocks.head(100)

    params = {}
    trouble_some_pdfs = {}

    master_stocks["annual_report_URL"] = master_stocks.apply(lambda row: _get_URL_google(row, trouble_some_pdfs),
                                                             axis=1)  # I commented this out to protect my 100 limit
    master_stocks.to_csv("my_stocks_master_with_URLs_and_PDF_paths.csv")
    master_stocks.to_csv(
        "EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv")  # although at this point you DON'T have the PDF paths!

    master_stocks = master_stocks.dropna()  # some URLs weren't retrievable - get rid of them

    # uses ASYNC_URL to download PDF and saves the unsuccessful ones to trouble_some_pdfs
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
    trouble_some_pdfs = {}
    symbols_only_data = main()
    SearchScrape(symbols_only_data.iloc[1])

