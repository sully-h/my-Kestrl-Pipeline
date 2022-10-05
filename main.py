import datetime

import pandas as pd
import camelot

from pdf_find_pages import pdf_find_pages
from camelot import read_pdf
from base import Base
from camelot_table_scraper import CamelotTableScraper

from finance_revenue_getter import _get_total_revenue_yahoo_query
from driver_pdf_downloader import MyPDFDownloader  # opens a blank window for some reason...
from pdf_find_pages import pdf_find_pages
import boto3

from pdf_URL_getter import _get_URL_google, download_annual_report, SearchScrape #_get_URL_scraper_api

from _extract import Extract
from tablescraper import TableScraper



class Extract():
    '''initialises the data from internet and makes sure everything is there'''

    def __init__(self, **kwargs):
        '''initialises the data from internet and makes sure everything is there'''
        self.kwargs = kwargs
        self.trouble_some_pdfs = {}  # for catching and debugging the problem stock tickers

        #upload_s3 = self.kwargs.get("upload_s3_filename", False)
        # TODO option to get read data from s3 bucket instead of local path (unless the s3 bucket can just be expressed as a path)
        # TODO if a list of paths is passed in, merge them (on which column?) OR save for elsewhere?
        self.master_stocks = Base.receive_data(self,**kwargs)
        self.master_stocks = self.master_stocks.dropna()
        #self.master_stocks = pd.read_csv(path).dropna()  # perhaps this should be read from s3

        self.check_is_revenue_col()  # check if ["Total Revenue 20/21"] is a column and place it there
        self.check_is_URL_col()  # check if ["annual_report_URL"] is a column and place it there
        self.check_is_PDF_path()  # check if ["PDF_path"] is a column and place it there
        # TODO validate the data between stages properly

        print('Revenue --> URL --> PDF downloads complete (Extract)')
        # if upload_s3: # not necessary because I have all the data after the PDF step
        #     self.upload_s3(s3_filename=upload_s3) # I can change this if I want tbh

    # def receive_data(self):
    #     if self.kwargs.get("dataframe") is None and self.kwargs.get("path") is None:
    #         raise ValueError("You must pass in one of dataframe or path")
    #     if self.kwargs.get("dataframe") is not None and self.kwargs.get("path") is not None:
    #         raise ValueError("You must pass in either the dataframe or the path to a CSV but not both")
    #
    #     if self.kwargs.get("dataframe") is not None:  # if it's a dataframe that's been passed in - use that
    #         return self.kwargs.get("dataframe")
    #     else:
    #         return pd.read_csv(self.kwargs.get("path"))

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
            self.upload_s3(s3_filename="intermediate data sources/EOD_LSE_postget_Revenue.csv", **self.kwargs)

    def check_is_URL_col(self):
        '''check if the annual_report_URL is present as a column and if not get it'''
        try:
            self.master_stocks["annual_report_URL"]
        except:
            try:  # use SERPAPI first to get annual reports
                self.master_stocks["annual_report_URL"] = self.master_stocks.apply(lambda row: SearchScrape(row, trouble_some_pdfs=self.trouble_some_pdfs).url, axis=1)  # I wonder if self.trouble_some_pdfs is getting mutated from within SearchScrape

                self.upload_s3(
                    s3_filename="intermediate data sources/EOD_LSE_with_annual_report_URL.csv", **self.kwargs)  # should I indicate how I got the data? I think that's more of a logging thing
            except: # if SERPAPI doesn't work for some reason, fall back on using the Google JSON Api (100 searches a day)
                self.master_stocks["annual_report_URL"] = self.master_stocks.apply(
                    lambda row: _get_URL_google(row, self.trouble_some_pdfs), axis=1)
                self.upload_s3(s3_filename="intermediate data sources/EOD_LSE_with_annual_report_URL.csv", **self.kwargs)

    def check_is_PDF_path(self):
        '''check if the PDF_path is present as a column and if not get it'''
        #    """these PDFs will be in s3 and this method will need to discover them or put them there"""
        try:
            self.master_stocks["PDF_path"]
        except KeyError:
            self.master_stocks["PDF_path"] = self.master_stocks.apply(
                lambda row: download_annual_report(row, trouble_some_pdfs=self.trouble_some_pdfs), axis=1)
            self.upload_s3(s3_filename="intermediate data sources/EOD_LSE_postget_Revenue_URL_PDFdownload.csv", **self.kwargs)

    def upload_s3(self, **kwargs): # how do I make it so you can make this choice to upload at runtime/in the console?
        # TODO how to avoid overwriting my data with test data (one row), I want to include a prefix that can be set at run-time
        post_internet_data_file_name = kwargs.get("s3_filename",
                                                  None) # this never gets called because I always use s3_filename as a parameter
        if kwargs.get("prefix"):
            post_internet_data_file_name = kwargs['prefix'] + post_internet_data_file_name

        s3 = boto3.resource("s3")
        bucket = s3.Bucket("kestrl-data-intern")
        if post_internet_data_file_name:
            self.master_stocks.to_csv(post_internet_data_file_name)
            bucket.upload_file(Filename=f"{post_internet_data_file_name}",
                               Key=f"intermediate-data/{str(datetime.datetime.now()).replace(':', ' ')}-{post_internet_data_file_name}")
        else:
            pass # if s3_filename isn't passed in
        # TODO delete the csv file from local file?


class TableScraper():
    '''use extracted data (PDFs) to extract tables'''

    def __init__(self, **kwargs):
        # if kwargs.get("dataframe") is not None or kwargs.get("path") is not None: # only if I'm testing TableScraper directly
        #     super().__init__(**kwargs) # provides self.receive_data() which allows dataframe OR path to be parameter
        # TODO find a way to allow the user to test TableScraper independently
        if kwargs.get("test", False):
            self.master_stocks=Base.receive_data(self, **kwargs)

        # the only reason I use the kwargs is for Base to establish whether its a dataframe or path passed in!
        self.master_stocks = Extract(**kwargs).master_stocks  # ensure the data is fully extracted # the choice to upload has been removed here - it's always done now unless s3_filename is changed in each Extract method

        self.kwargs = kwargs
        self._camelot_scraper = None

        # remember where the original path was so I can append a phrase and save it elsewhere
        #self.path = path

        print('Initialised TableScraper')


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

    def add_revenue_page_numbers(self):
        '''add a column containing page numbers where the revenue appears & total pages in the PDF'''
        PDF_page_hits_rev_df = self.master_stocks.apply(
            lambda row: pdf_find_pages(xsearch_string=row["Total Revenue 20/21"], row=row), result_type='expand',
            axis=1)  # expands into two new series

        df_with_rev_pages = pd.concat([self.master_stocks, PDF_page_hits_rev_df], axis='columns')
        df_with_rev_pages.to_csv("EOD_LSE_merged_Rev_page_numbers.csv")  # keep the new data

        return df_with_rev_pages  # adds 'Rev PDF Page Numbers' and 'PDF_total_pages' columns


class Application:
    pass


if __name__ == '__main__':
    one_company = pd.read_csv("intermediate data sources/EOD_LSE_filtered_master_data.csv").sample(n=1, random_state=47)
    one_company_extracted = TableScraper(dataframe=one_company, prefix="minitest-")
    cam_scraper = one_company_extracted.get_csv_tables_from_pdf_camelot()
    #url_search_scrape = Extract(path="tests/EOD_LSE_with_Revenue.csv", upload_s3_filename="minitest-extract-post-SCRAPERAPI-code.csv")
    # # TablePDF = Extract(path="EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv")
    # table_scraper = TableScraper("small_total_rev_included.csv")
    # table_scraper.get_csv_tables_from_pdf_camelot()
    # # extract_save_s3 = Extract("intermediate data sources/EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv", upload_s3=True)

