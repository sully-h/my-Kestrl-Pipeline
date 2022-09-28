import numpy as np
import pathlib

import re
import PyPDF2

# TODO save all the xpages into a directory for future  --> avoid extracting the same page twice unless the revenue is missing for the total document

# TODO implement logger and catch stack trace - why do not all PDFs


import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(messages)s')

file_handler = logging.FileHandler('thirdlog.log')
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
# I don't want the formatting on this one to change

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def pdf_find_pages(xfile_pdf=None, xsearch_string=None, row=None):
    '''searches PDF for number - strips trailing 0s if Number doesn't exist'''
    xfile_pdf = row["PDF_path"]
    # xsearch_string=row["Total Revenue 20/21"]

    xsearch_string = str(xsearch_string)
    PDF_total_pages, Rev_PDF_Page_Numbers = _pdf_find_pages(xsearch_string=xsearch_string, row=row).values()

    if Rev_PDF_Page_Numbers:  # if the search term appears at least once
        return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': Rev_PDF_Page_Numbers}
    else:  # remove trailing 0s and search again
        xsearch_string = xsearch_string.rstrip("0").rstrip(".")
        PDF_total_pages, Rev_PDF_Page_Numbers = _pdf_find_pages(xsearch_string=xsearch_string, row=row).values()
        if Rev_PDF_Page_Numbers:
            return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': Rev_PDF_Page_Numbers}
        else:  # remove the 0s in the integer and search once more
            xsearch_string = xsearch_string.rstrip("0")
            PDF_total_pages, Rev_PDF_Page_Numbers = _pdf_find_pages(xsearch_string=xsearch_string, row=row).values()

    return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': np.nan}


def _pdf_find_pages(xfile_pdf=None, xsearch_string=None, row=None, ignore_case=False):
    '''
    find page(s) on which a given text is located in a pdf
    input: pdf file and the string to search
    (string to search can be in a regex like 'references\n')

    N.B:
    results need to be checked
    in case of pdf whose page numbers are not zero indexed ,
    the results seems off (by one page)
    '''
    xfile_pdf = row["PDF_path"]
    # xsearch_string = row["Total Revenue 20/21"]

    try:
        xsearch_string = str(xsearch_string)
        xlst_res = []

        xreader = PyPDF2.PdfFileReader(xfile_pdf)

        is_fresh_extracted = False
        is_hit = False

        for xpage_nr, xpage in enumerate(xreader.pages):
            # first check if there is a text file with the page already
            if pathlib.Path(
                    f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt").is_file():  # check if this page exists as a text file in storage
                with open(f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt", 'r',
                          encoding='utf-8') as annual_report_page:
                    xpage_text = annual_report_page.read()
            else:  # if it doesn't exist, extract the page and save it
                xpage_text = xpage.extractText()
                save_extracted_page(row, xpage_text=xpage_text, xpage_nr=xpage_nr)
                is_fresh_extracted = True

            # search func
            is_hit, xlst_res = re_search(xsearch_string=xsearch_string, xpage_text=xpage_text, xpage_nr=xpage_nr,
                                         xlst_res=xlst_res)  # was there a hit on this page? If so add it to xlst_res

            if is_hit:  # if the search term was found, move onto next page
                pass
            else:  # if there's no hit, try extracting the page fresh
                if is_fresh_extracted:  # if the page was already extracted today, just move onto the next page since it's likely absent from the page
                    pass
                else:  # only extract if it hasn't just been extracted
                    xpage_text = xpage.extractText()
                    save_extracted_page(row, xpage_text=xpage_text, xpage_nr=xpage_nr)

                    is_hit, xlst_res = re_search(xsearch_string=xsearch_string, xpage_text=xpage_text,
                                                 xpage_nr=xpage_nr, xlst_res=xlst_res)

            is_fresh_extracted = False  # move onto the next page which hasn't been extracted yet

        str_page_hits = ', '.join(str(elmn) for elmn in xlst_res)
        return {'PDF_total_pages': xreader.numPages,
                'Rev PDF Page Numbers': str_page_hits}  # 'PDF_page_hits': xlst_res I removed this from the dict

    except:
        # logger.exception(f"Why is a msg required?{row['Symbol']}") # this causes an empty log file ??
        logger.error(f"Why is a msg required?{row['Symbol']}")
        return {'PDF_total_pages': np.nan, 'Rev PDF Page Numbers': np.nan}


def re_search(xsearch_string=None, xpage_text=None, ignore_case=False, xpage_nr=None, xlst_res: list = None):
    xhits = None
    if ignore_case == False:
        xhits = re.search(xsearch_string, xpage_text.lower())  # search on the page
    else:
        xhits = re.search(xsearch_string, xpage_text.lower(), re.IGNORECASE)

    if xhits:  # if the search is successful
        xlst_res.append(xpage_nr)
        return True, xlst_res

    return False, xlst_res


def save_extracted_page(row, xpage_text=None, xpage_nr=None):
    pathlib.Path(f"annual-reports-text-pages/{row['Symbol']}-pages").mkdir(parents=True,
                                                                           exist_ok=True)  # ensure there is a subdirectory for this stock ticker to store its pages txt in
    with open(f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt", 'w',
              encoding='utf-8') as annual_report_page:
        annual_report_page.write(xpage_text)
        logging.info(f"page saved into {row['Symbol']}-pages/page-nr-{xpage_nr}.txt")
