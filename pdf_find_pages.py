import numpy as np

import re
import PyPDF2


def _pdf_find_pages(xfile_pdf, xsearch_string, ignore_case=False):
    '''
    find page(s) on which a given text is located in a pdf
    input: pdf file and the string to search
    (string to search can be in a regex like 'references\n')

    N.B:
    results need to be checked
    in case of pdf whose page numbers are not zero indexed ,
    the results seems off (by one page)
    '''

    xsearch_string = str(xsearch_string)
    xlst_res = []

    xreader = PyPDF2.PdfFileReader(xfile_pdf)

    for xpage_nr, xpage in enumerate(xreader.pages):
        xpage_text = xpage.extractText()
        xhits = None
        if ignore_case == False:
            xhits = re.search(xsearch_string, xpage_text.lower())
        else:
            xhits = re.search(xsearch_string, xpage_text.lower(), re.IGNORECASE)

        if xhits:
            xlst_res.append(xpage_nr)

    str_page_hits = ', '.join(str(elmn) for elmn in xlst_res)
    return {'PDF_total_pages': xreader.numPages,
            'Rev PDF Page Numbers': str_page_hits}  # 'PDF_page_hits': xlst_res I removed this from the dict


def pdf_find_pages(xfile_pdf, xsearch_string):
    '''searches PDF for number - strips trailing 0s if Number doesn't exist'''
    PDF_total_pages, Rev_PDF_Page_Numbers = _pdf_find_pages(xfile_pdf, xsearch_string).values()

    if Rev_PDF_Page_Numbers: # if the search term appears at least once
        return {'PDF_total_pages':PDF_total_pages, 'Rev PDF Page Numbers': Rev_PDF_Page_Numbers}
    else: # remove trailing 0s and search again
        xsearch_string = xsearch_string.rstrip("0")
        PDF_total_pages, Rev_PDF_Page_Numbers = _pdf_find_pages(xfile_pdf, xsearch_string).values()
        if Rev_PDF_Page_Numbers:
            return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': Rev_PDF_Page_Numbers}

    return {'PDF_total_pages': PDF_total_pages, 'Rev PDF Page Numbers': np.nan}
