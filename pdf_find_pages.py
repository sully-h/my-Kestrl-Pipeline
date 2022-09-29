import pandas as pd # need this one for testing the file in console
import numpy as np
import pathlib

import re
import PyPDF2

# TODO implement logger and catch stack trace - why do not all PDFs

# TODO write a function that can adjust the search term until there's a match or give up and move onto the next page :sometimes there are commas, sometimes the precision in the document is lower so I have to remove sig figs

import logging

logging.basicConfig(filename='fourthlog.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def pdf_find_pages(row=None, **kwargs):
    '''searches PDF for number - strips trailing 0s if Number doesn't exist'''
    xfile_pdf = kwargs.get("xfile_pdf", row["PDF_path"]) # if xfile_pdf is passed in used
    xsearch_string = kwargs.get("xsearch_string", row["Total Revenue 20/21"])
    xsearch_string = str(xsearch_string)

    PDF_total_pages, Rev_PDF_Page_Numbers = _pdf_find_pages(xsearch_string=xsearch_string, row=row).values()

    # function to change xsearch_str and search again
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


def _pdf_find_pages(row=None, **kwargs):
    '''
    find page(s) on which a given text is located in a pdf
    input: pdf file and the string to search
    (string to search can be in a regex like 'references\n')

    N.B:
    results need to be checked
    in case of pdf whose page numbers are not zero indexed ,
    the results seems off (by one page)
    '''
    xfile_pdf = kwargs.get("xfile_pdf", row["PDF_path"])  # if xfile_pdf is passed in used
    xsearch_string = kwargs.get("xsearch_string", row["Total Revenue 20/21"])

    try: # analysing the entire PDF
        xsearch_string = str(xsearch_string)
        xlst_res = []

        xreader = PyPDF2.PdfFileReader(xfile_pdf)

        # the generator should start here and encompass the for loop
        is_fresh_extracted = False # TODO use a generator function on each page {checks for hit (yield is_hit) --> checks for file (yields is_fresh_extracted false) --> extracts it (yields is_fresh_extracted true)}
        is_hit = False

        for xpage_nr, xpage in enumerate(xreader.pages): # for each page: extracts it (if there wasn't a hit) and gives the new hit
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
            elif not is_hit: # trim the xsearch_string and search again BUT THIS IS ALREADY IN THE TOP THING
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

        #str_page_hits = ', '.join(str(elmn) for elmn in xlst_res)
        return {'PDF_total_pages': xreader.numPages,
                'Rev PDF Page Numbers': ', '.join(str(elmn) for elmn in xlst_res)}  # 'PDF_page_hits': xlst_res I removed this from the dict

    except:
        # logger.exception(f"Why is a msg required?{row['Symbol']}") # this causes an empty log file ??
        logging.error(f"{row['Symbol']}", exc_info=1)
        return {'PDF_total_pages': np.nan, 'Rev PDF Page Numbers': np.nan}


def re_search(xsearch_string=None, xpage_text=None, xpage_nr=None, xlst_res: list = None):
    xhits = None
    xhits = re.search(xsearch_string, xpage_text.lower())  # search on the page # returns a <re.Match object; match='260.0'>

    #print(f"xhits:  {xhits}, '\n', xsearch_string:  {xsearch_string}, '\n', xpage_text.lower[:20]:  {xpage_text.lower()[:20]}")
    logging.info(msg=f"xhits:  {xhits}, '\n', xsearch_string:  {xsearch_string}, '\n', xpage_text.lower[:20]:  {xpage_text.lower()[:20]}")

    if xhits:  # if the search is successful
        xlst_res.append(xpage_nr)
        return True, xlst_res

    return False, xlst_res # if its False, either trim the xsearch_string or extract the file from fresh


def save_extracted_page(row, xpage_text=None, xpage_nr=None):
    pathlib.Path(f"annual-reports-text-pages/{row['Symbol']}-pages").mkdir(parents=True,
                                                                           exist_ok=True)  # ensure there is a subdirectory for this stock ticker to store its pages txt in
    with open(f"annual-reports-text-pages/{row['Symbol']}-pages/page-nr-{xpage_nr}.txt", 'w',
              encoding='utf-8') as annual_report_page:
        annual_report_page.write(xpage_text)
        logging.info(f"page saved into {row['Symbol']}-pages/page-nr-{xpage_nr}.txt")


if __name__ =='__main__':
    data = pd.read_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue_manually_edited.csv")
    row = data.iloc[3]
    total_pages, rev_pages = _pdf_find_pages(row=row).values()