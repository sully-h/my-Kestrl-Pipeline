import pandas as pd  # need this one for testing the file in console
import numpy as np
import pathlib

import re
import PyPDF2

import math
import ballpark

# TODO implement logger and catch stack trace - why do not all PDFs
import traceback
# TODO write a function that can adjust the search term until there's a match or give up and move onto the next page :sometimes there are commas, sometimes the precision in the document is lower so I have to remove sig figs

import logging

from readupload import ReadUpload

logging.basicConfig(filename='fourthlog.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class PDFUtility:

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.master_stocks = ReadUpload.receive_data(self, **kwargs)

        self.is_hit=False
        self.is_fresh_extracted=False

    def add_revenue_page_numbers(self):  # TODO move this to the camelot scraper or nah?
        '''add a column containing page numbers where the revenue appears & total pages in the PDF'''
        PDF_page_hits_rev_df = self.master_stocks.apply(lambda row: PDFUtility.pdf_find_pages(self, xsearch_string=row["Total Revenue 20/21"], row=row),
            result_type='expand', axis=1)  # expands into two new series

        total_df_with_rev_pages = pd.concat([self.master_stocks, PDF_page_hits_rev_df], axis='columns')
        # TODO upload this data to s3 and ensure prefixes can be added to it
        ReadUpload.upload_s3(self, **self.kwargs)
        print(f"Uploaded to {self.kwargs.get('prefix')}intermediate data sources/{self.kwargs.get('s3_filename')}")
        total_df_with_rev_pages.to_csv("EOD_LSE_merged_Rev_page_numbers.csv")  # keep the new data

        return total_df_with_rev_pages  # adds 'Rev PDF Page Numbers' and 'PDF_total_pages' columns

    def pdf_find_pages(self, row=None, **kwargs):
        '''searches PDF for number - strips trailing 0s if Number doesn't exist''' # TODO delete all this
        xfile_pdf = kwargs.get("xfile_pdf", row["PDF_path"])
        xsearch_string = kwargs.get("xsearch_string", row["Total Revenue 20/21"])
        xsearch_string = str(xsearch_string)  # TODO make search function that iterates through changes # this preprocessing should just happen in _pdf_find_pages

        PDF_total_pages, Rev_PDF_Page_Numbers = PDFUtility._pdf_find_pages( xsearch_string=xsearch_string, row=row).values() #

        # function to change xsearch_str and search again
        # if Rev_PDF_Page_Numbers:  # if the search term appears at least once
        #     return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': Rev_PDF_Page_Numbers}
        # else:  # remove trailing 0s and search again
        #     xsearch_string = xsearch_string.rstrip("0").rstrip(".")
        #     PDF_total_pages, Rev_PDF_Page_Numbers = PDFUtility._pdf_find_pages(xsearch_string=xsearch_string, row=row).values() #
        #     if Rev_PDF_Page_Numbers:
        #         return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': Rev_PDF_Page_Numbers}
        #     else:  # remove the 0s in the integer and search once more
        #         xsearch_string = xsearch_string.rstrip("0")
        #         PDF_total_pages, Rev_PDF_Page_Numbers = PDFUtility._pdf_find_pages( xsearch_string=xsearch_string, row=row).values() #

        return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': np.nan}

    def _pdf_find_pages(row=None, **kwargs):
        '''
        find which page(s) Revenue is located in a pdf and return in dictionary
        input: pdf file and the string to search
        (string to search can be in a regex like 'references\n')

        NB: Results may be off by one
        '''
        # TODO this whole function needs to be improved
        xsearch_string = kwargs.get("xsearch_string", row["Total Revenue 20/21"])
        xfile_pdf = kwargs.get("xfile_pdf", row["PDF_path"])

        xlst_res = []
        try:  # analysing the entire PDF
            xsearch_string = str(xsearch_string)

            xreader = PyPDF2.PdfFileReader(xfile_pdf)

            is_fresh_extracted = False  # TODO use a generator function on each page {checks for hit (yield is_hit) --> checks for file (yields is_fresh_extracted false) --> extracts it (yields is_fresh_extracted true)}
            is_hit = False

            for xpage_nr, xpage in enumerate(xreader.pages):  # for each page: extracts it (if there wasn't a hit) and gives the new hit
                # first check if there is a text file with the page already and read it into xpage_text
                is_fresh_extracted, xpage_text = PDFUtility.read_xpage(row, xpage=xpage, xpage_nr=xpage_nr)
                PDFUtility.save_extracted_page(row, xpage_text=xpage_text, xpage_nr=xpage_nr) # now its getting saved

                is_hit=False # needs to be a list if I want to have it mutable and want it to change and respond to functions?
                # search func
                while True: # searching xpage_text (one page) for xsearch_string
                    page_hit = PDFUtility.check_revenue_orig(xsearch_string=xsearch_string, xtext=xpage_text, xpage_nr=xpage_nr) # checkrevenue1 next(checkrevenue)
                    if page_hit:
                        xlst_res.append(page_hit)
                        break
                    page_hit = PDFUtility.check_revenue_1(xsearch_string=xsearch_string, xtext=xpage_text, xpage_nr=xpage_nr)
                    if page_hit:
                        xlst_res.append(page_hit)
                        break
                    page_hit = PDFUtility.check_revenue_2(xsearch_string=xsearch_string, xtext=xpage_text, xpage_nr=None)
                    if page_hit:
                        xlst_res.append(page_hit)
                        break
                    xlst_res.append(page_hit) # page_hit=None if the page is devoid of all revenues

            return {'PDF_total_pages': xreader.numPages,
                    'All Pages' : ', '.join(
                        str(elmn) for elmn in xlst_res),
                    'Rev PDF Page Numbers': ', '.join(
                        str(elmn) for elmn in xlst_res if elmn is not None)}  # 'PDF_page_hits': xlst_res I removed this from the dict

        except Exception as e:
            # logger.exception(f"Why is a msg required?{row['Symbol']}") # this causes an empty log file ??
            logging.error(f"{row['Symbol']}", exc_info=1)
            print(traceback.format_exc())
            return {'PDF_total_pages': np.nan, 'Rev PDF Page Numbers': np.nan}


    def check_revenue_orig(xsearch_string=None, xtext=None, xpage_nr=None):
        if re.search(xsearch_string, xtext):
            return xpage_nr
        else:
            return None

    def check_revenue_1(xsearch_string=None, xtext=None, xpage_nr=None):
        xsearch_string = str(round(float(xsearch_string) / 10**6, 1))

        if re.search(xsearch_string, xtext):
            return xpage_nr
        else:
            return None

    def check_revenue_2(xsearch_string=None, xtext=None, xpage_nr=None):
        xsearch_string = str(round(float(xsearch_string) / 10 ** 6, 1))
        #further processing like adding commas
        if re.search(xsearch_string, xtext):
            return xpage_nr
        else:
            return None

    def read_xpage(row, xpage=None, xpage_nr=None):
        ''' first check if there is a text file with the page already and read it into xpage_text'''

        if pathlib.Path(f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt").is_file():  # check if this page exists as a text file in storage
            with open(f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt", 'r',encoding='utf-8') as annual_report_page:
                is_fresh_extracted = False
                xpage_text = annual_report_page.read()
            return is_fresh_extracted, xpage_text
        else:  # if it doesn't exist, extract the page and save it
            is_fresh_extracted = True
            xpage_text = xpage.extractText()
            return is_fresh_extracted, xpage_text

    # Deprecated
    def re_search(xsearch_string=None, xpage_text=None, xpage_nr=None, xlst_res: list = None): # remove self bcos this is static?
        xhits = None
        xhits = re.search(xsearch_string, xpage_text.lower())  # search on the page # returns a <re.Match object; match='260.0'>

        # print(f"xhits:  {xhits}, '\n', xsearch_string:  {xsearch_string}, '\n', xpage_text.lower[:20]:  {xpage_text.lower()[:20]}")
        logging.info(msg=f"xhits:  {xhits}, '\n', xsearch_string:  {xsearch_string}, '\n', xpage_text.lower[:20]:  {xpage_text.lower()[:20]}")

        if xhits:  # if the search is successful
            xlst_res.append(xpage_nr)
            is_xhits = True
            return is_xhits, xlst_res
        is_xhits = False

        xsearch_string = round(xsearch_string / 10**6)
        yield is_xhits, xlst_res  # if its False, either trim the xsearch_string or extract the file from fresh

    def save_extracted_page(row, xpage_text=None, xpage_nr=None):
        pathlib.Path(f"annual-reports-text-pages/{row['Symbol']}-pages").mkdir(parents=True,
                                                                               exist_ok=True)  # ensure there is a subdirectory for this stock ticker to store its pages txt in
        with open(f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt", 'w',
                  encoding='utf-8') as annual_report_page:
            annual_report_page.write(xpage_text)
            logging.info(f"page saved into {row['Symbol']}-pages/page-nr-{xpage_nr}.txt")

    # from ballpark github
    def human(value, digits=2):
        return '{:,}'.format(round(value, digits))


if __name__ == '__main__':
    data = pd.read_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue_manually_edited.csv")
    data_post = pd.read_csv("existing-PDFS-57intermediate data sources/EOD_LSE_postget_Revenue_URL_PDFdownload.csv")
    pdf_utility = PDFUtility(dataframe=data_post)
    added_rev = pdf_utility.add_revenue_page_numbers()
    # row = data.iloc[3]
    # # total_pages, rev_pages = _pdf_find_pages(row=row).values()
    # rounded_nums = [ballpark.business(1234, precision=i, prefix=False) for i in range(int(math.log10(1234)), 0, -1)]
