import pandas as pd
import camelot

from camelot import read_pdf

from finance_revenue_getter import main_revenue_getter
from driver_pdf_downloader import MyPDFDownloader  # opens a blank window for some reason...
from pdf_parser import pdf_find_text
from camelot import read_pdf
# from pdf_reader import camelot.read_pdf
from parsr_getter import parsr_table_maker

from pdf_URL_getter import get_URL, download_annual_report


class StorePDFTables:
    # TODO everything is inside the __init__ scope -> how should I pass around self.master_stocks between my functions?
    def __init__(self):
        '''initialises the data and makes sure everything is there'''
        # some PDFs were irretrievable so you can drop those tickers here
        self.trouble_some_pdfs = {}
        self.master_stocks = pd.read_csv("my_stocks_master.csv")  # stock ticker together with revenue
        self.master_stocks = pd.read_csv("my_stocks_master_with_URLs_and_PDF_paths.csv")
        self.master_stocks = pd.read_csv(
            "EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv").dropna()  # perhaps this should be read from s3

        self.total_revenue_tables_dict = {}  # don't need this since no camelot tables

        self.check_is_revenue_col() # check if ["Total Revenue 20/21"] is a column and place it there
        self.check_is_URL_col() # check if ["annual_report_URL"] is a column and place it there
        self.check_is_PDF_path() # check if ["PDF_path"] is a column and place it there

    def check_is_revenue_col(self):
        # yahoo revenue data doesn't always match the annual reports so needs work
        try:  # check if revenue as a column exists in the data
            self.master_stocks["Total Revenue 20/21"]
        # get_annual_report is now a class method of MyPDFDownloader .. but I don't need selenium to download these for now
        # get_annual_report()  # open and download pdfs # I will probably change this to a headless selenium OR curl function OR requests.get OR Wget
        except KeyError:  # if it doesn't get the data from yahoo
            self.master_stocks = main_revenue_getter(self.master_stocks,
                                                     "EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv")

    def check_is_URL_col(self):
        try:
            self.master_stocks["annual_report_URL"]
        except:
            self.master_stocks.apply(lambda row: get_URL(row, trouble_some_pdfs=self.trouble_some_pdfs), axis=1)

    def check_is_PDF_path(self):

        """these PDFs will be in s3 and this method will need to discover them or put them there"""
        try:
            self.master_stocks["PDF_path"]
        except KeyError:
            self.master_stocks.apply(lambda row: download_annual_report(row, trouble_some_pdfs=self.trouble_some_pdfs), axis=1)

    def get_csv_tables_from_pdf(self):
        """produce CSV files for each table"""
        parsr_table_maker(self.master_stocks)
        # parsr_table_s3_uploader



if __name__ == '__main__':
    TablePDF = StorePDFTables()
