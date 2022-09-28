import pandas as pd
import camelot

from pdf_find_pages import pdf_find_pages
from camelot import read_pdf
from camelot_table_scraper import CamelotTableScraper

from finance_revenue_getter import main_revenue_getter
from driver_pdf_downloader import MyPDFDownloader  # opens a blank window for some reason...
from pdf_find_pages import pdf_find_pages
from camelot import read_pdf
# from pdf_reader import camelot.read_pdf
#from parsr_getter import parsr_table_maker

from pdf_URL_getter import get_URL_google, download_annual_report


class Extract:
    '''initialises the data from internet and makes sure everything is there'''

    # TODO everything is inside the __init__ scope -> how should I pass around self.master_stocks between my functions?
    def __init__(self, path):
        '''initialises the data from internet and makes sure everything is there'''
        # TODO if a list of paths is passed in, merge them (on which column?) OR save for elsewhere
        self.master_stocks = pd.read_csv(path).dropna()  # perhaps this should be read from s3
        self.total_revenue_tables_dict = {}  # don't need this since no camelot tables

        self.check_is_revenue_col()  # check if ["Total Revenue 20/21"] is a column and place it there
        self.check_is_URL_col()  # check if ["annual_report_URL"] is a column and place it there
        self.check_is_PDF_path()  # check if ["PDF_path"] is a column and place it there
        # TODO validate the data between stages properly and collect that data
        self.trouble_some_pdfs = {}  # for catching and debugging the problem stock tickers

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
            self.master_stocks = main_revenue_getter(self.master_stocks,
                                                     "EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv")
            self.catch_error()

    def check_is_URL_col(self):
        '''check if the annual_report_URL is present as a column and if not get it'''
        try:
            self.master_stocks["annual_report_URL"]
        except:
            self.master_stocks.apply(lambda row: get_URL_google(row, trouble_some_pdfs=self.trouble_some_pdfs), axis=1)

    def check_is_PDF_path(self):
        '''check if the PDF_path is present as a column and if not get it'''
        #    """these PDFs will be in s3 and this method will need to discover them or put them there"""
        try:
            self.master_stocks["PDF_path"]
        except KeyError:
            self.master_stocks.apply(lambda row: download_annual_report(row, trouble_some_pdfs=self.trouble_some_pdfs),
                                     axis=1)


class TableScraper:
    '''use extracted data (PDFs) to extract tables'''

    def __init__(self, path):
        self.master_stocks = Extract(path).master_stocks

    def add_revenue_page_numbers(self):
        PDF_page_hits_rev_df = self.master_stocks.apply(lambda row: pdf_find_pages(xsearch_string=row["Total Revenue 20/21"],row=row), result_type='expand', axis=1) # expands into two new series

        df_with_rev_pages = pd.concat([self.master_stocks, PDF_page_hits_rev_df], axis='columns')
        df_with_rev_pages.to_csv("EOD_LSE_merged_Rev_page_numbers.csv") # keep the new data

        return df_with_rev_pages  # adds 'Rev PDF Page Numbers' and 'PDF_total_pages' columns

    # TODO extend these methods to an interface because each of these libraries can return markdown and json too
    def get_csv_tables_from_pdf_camelot(self):
        """use Camelot to parse PDF documents into CSV files"""
        self.master_stocks = self.add_revenue_page_numbers()  # I need the page numbers for Camelot to run faster # this also captures the text files that are produced during extraction
        _camelot_scraper = CamelotTableScraper(self.master_stocks)

    def get_csv_tables_from_pdf_tabula(self):
        # TODO use Tabula to produce CSV files for each table
        '''use Tabula to parse PDF documents into CSV files'''
        pass

    def get_csv_tables_from_pdf_parsr(self):
        # TODO use Parsr to produce CSV files
        """use Parsr(Docker Image) to produce CSV files for each table"""
        #parsr_table_maker(self.master_stocks)
        # parsr_table_s3_uploader


class Application:
    pass


if __name__ == '__main__':
    # TablePDF = Extract(path="EOD_LSE_merged_filtered_with_URLS_PDF_paths.csv")
    table_scraper = TableScraper("EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv")
    table_scraper.get_csv_tables_from_pdf_camelot()
