import datetime

import pandas as pd
import camelot

from pdf_find_pages import pdf_find_pages
from camelot import read_pdf
from camelot_table_scraper import CamelotTableScraper

from finance_revenue_getter import main_revenue_getter, _get_total_revenue_yahoo_query
from driver_pdf_downloader import MyPDFDownloader  # opens a blank window for some reason...
from pdf_find_pages import pdf_find_pages
from camelot import read_pdf
# from pdf_reader import camelot.read_pdf
# from parsr_getter import parsr_table_maker

import boto3

from pdf_URL_getter import _get_URL_google, download_annual_report, SearchScrape #_get_URL_scraper_api

from extract import Extract
from tablescraper import TableScraper


class Extract:
    '''initialises the data from internet and makes sure everything is there'''

    def __init__(self, path, **kwargs):
        '''initialises the data from internet and makes sure everything is there'''
        self.kwargs = kwargs
        self.trouble_some_pdfs = {}  # for catching and debugging the problem stock tickers

        upload_s3 = self.kwargs.get("upload_s3_filename", False)
        # TODO option to get read data from s3 bucket instead of local path (unless the s3 bucket can just be expressed as a path)
        # TODO if a list of paths is passed in, merge them (on which column?) OR save for elsewhere?
        self.master_stocks = pd.read_csv(path).dropna()  # perhaps this should be read from s3

        self.check_is_revenue_col()  # check if ["Total Revenue 20/21"] is a column and place it there
        self.check_is_URL_col()  # check if ["annual_report_URL"] is a column and place it there
        self.check_is_PDF_path()  # check if ["PDF_path"] is a column and place it there
        # TODO validate the data between stages properly and collect that data

        if upload_s3:
            self.upload_s3(s3_filename=upload_s3) # I can change this if I want tbh

    def catch_error(self):
        '''Validate the data at extraction and catch errors for bad extractions'''
        pass

    def check_is_revenue_col(self):
        '''check if the Total Revenue 20/21 is present as a column and if not get it'''
        # yahoo revenue data doesn't always match the annual reports so needs work
        try:  # check if revenue as a column exists in the data
            self.master_stocks["Total Revenue 20/21"]
        # get_annual_report is now a class method of MyPDFDownloader .. but I don't need selenium to download these for now
        # get_annual_report()  # open and download pdfs # I will probably change this to a headless selenium OR curl function OR requests.get OR Wget
        except KeyError:  # if it doesn't get the data from yahoo
            self.master_stocks["Total Revenue 20/21"] = self.master_stocks.apply(
                lambda row: _get_total_revenue_yahoo_query(row), axis=1)
            self.catch_error()
            self.upload_s3(s3_filename="intermediate data sources/EOD_LSE_with_Revenue")

    def check_is_URL_col(self):
        '''check if the annual_report_URL is present as a column and if not get it'''
        try:
            self.master_stocks["annual_report_URL"]
        except:
            try:  # use SERPAPI first to get annual reports
                self.master_stocks["annual_report_URL"] = self.master_stocks.apply(lambda row: SearchScrape(row, trouble_some_pdfs=self.trouble_some_pdfs).url, axis=1)  # I wonder if self.trouble_some_pdfs is getting mutated from within SearchScrape

                self.upload_s3(
                    s3_filename="EOD_LSE_with_annual_report_URL")  # should I indicate how I got the data? I think that's more of a logging thing
            except: # if SERPAPI doesn't work for some reason, fall back on using the Google JSON Api (100 searches a day)
                self.master_stocks["annual_report_URL"] = self.master_stocks.apply(
                    lambda row: _get_URL_google(row, self.trouble_some_pdfs), axis=1)
                self.upload_s3(s3_filename="EOD_LSE_with_annual_report_URL")

    def check_is_PDF_path(self):
        '''check if the PDF_path is present as a column and if not get it'''
        #    """these PDFs will be in s3 and this method will need to discover them or put them there"""
        try:
            self.master_stocks["PDF_path"]
        except KeyError:
            self.master_stocks["PDF_path"] = self.master_stocks.apply(
                lambda row: download_annual_report(row, trouble_some_pdfs=self.trouble_some_pdfs), axis=1)
            self.upload_s3(s3_filename="EOD_LSE_PDF_path")

    def upload_s3(self, **kwargs):
        post_internet_data_file_name = kwargs.get("s3_filename",
                                                  "EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv")

        s3 = boto3.resource("s3")
        bucket = s3.Bucket("kestrl-data-intern")

        self.master_stocks.to_csv(post_internet_data_file_name)
        bucket.upload_file(Filename=f"{post_internet_data_file_name}",
                           Key=f"intermediate-data/{str(datetime.datetime.now()).replace(':', ' ')}-{post_internet_data_file_name}")
        # TODO delete the csv file from local file?


class TableScraper:
    '''use extracted data (PDFs) to extract tables'''

    def __init__(self, path, **kwargs):
        upload_s3 = kwargs.get("upload_s3", False)

        self.master_stocks = Extract(path, upload_s3=upload_s3).master_stocks  # ensure the data is fully extracted

        self._camelot_scraper = None

        # remember where the original path was so I can append a phrase and save it elsewhere
        self.path = path

    def add_revenue_page_numbers(self):
        '''add a column containing page numbers where the revenue appears & total pages in the PDF'''
        PDF_page_hits_rev_df = self.master_stocks.apply(
            lambda row: pdf_find_pages(xsearch_string=row["Total Revenue 20/21"], row=row), result_type='expand',
            axis=1)  # expands into two new series

        df_with_rev_pages = pd.concat([self.master_stocks, PDF_page_hits_rev_df], axis='columns')
        df_with_rev_pages.to_csv("EOD_LSE_merged_Rev_page_numbers.csv")  # keep the new data

        return df_with_rev_pages  # adds 'Rev PDF Page Numbers' and 'PDF_total_pages' columns

    # TODO extend these methods to an interface because each of these libraries can return markdown and json too
    def get_csv_tables_from_pdf_camelot(self):
        """use Camelot to parse PDF documents into CSV files"""
        try:
            self.master_stocks["Rev PDF Page Numbers"] # name of new column from add_revenue_page_numbers()
        except KeyError: # if the column's not there: add it
            self.master_stocks = self.add_revenue_page_numbers()  # I need the page numbers for Camelot to run faster # this also captures the text files that are produced during extraction

        # TODO add options for saving or not
        self._camelot_scraper = CamelotTableScraper(
            dataframe=self.master_stocks)  # should I pass in a path or a pd.DataFrame object? # returns a success massage, autosaves work


        self.master_stocks = self._camelot_scraper.master_stocks

    def get_csv_tables_from_pdf_tabula(self):
        # TODO use Tabula to produce CSV files for each table
        '''use Tabula to parse PDF documents into CSV files'''
        pass

    def get_csv_tables_from_pdf_parsr(self):
        # TODO use Parsr to produce CSV files
        """use Parsr(Docker Image) to produce CSV files for each table"""
        # parsr_table_maker(self.master_stocks)
        # parsr_table_s3_uploader


class Application:
    pass


if __name__ == '__main__':
    #url_search_scrape = Extract(path="tests/EOD_LSE_with_Revenue.csv", upload_s3_filename="minitest-extract-post-SCRAPERAPI-code.csv")
    # # TablePDF = Extract(path="EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv")
    # table_scraper = TableScraper("small_total_rev_included.csv")
    # table_scraper.get_csv_tables_from_pdf_camelot()
    # # extract_save_s3 = Extract("intermediate data sources/EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv", upload_s3=True)

