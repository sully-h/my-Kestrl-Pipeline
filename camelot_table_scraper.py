import pickle

import camelot
import pathlib

import numpy as np
import pandas as pd
from pdf_find_pages import pdf_find_pages


# tables = camelot.read_pdf('annual reports/visiomed_group_plan_strategique_veng (3).pdf')
# tables = camelot.read_pdf('annual reports/spatial-annual-report-2021.pdf')

class CamelotTableScraper():
    def __init__(self, master_stocks):
        # '''generates a column with Camelot <TableList n=n> data which can be exported''' # I decided to keep the df primitive
        """saves tables to CSV files and returns a success message or a failure message"""
        self.camelot_table_objects = {}

        self.master_stocks = master_stocks
        self.master_stocks.apply(lambda row: self._get_camelot_table(row), axis=1)

    def _get_camelot_table(self, row):
        """saves CSVs and pickle files for each PDF """
        # print(row["Symbol"])
        try:
            self.camelot_table_objects[row["Symbol"]] = camelot.read_pdf(row["PDF_path"], flavor='stream',
                                                                         pages=row['Rev PDF Page Numbers'])

            # save all the tables to CSVs inside a camelot-tables/ directory
            pathlib.Path(f"camelot-tables/{row['Symbol']}")
            self.camelot_table_objects[row["Symbol"]].export(
                f"camelot-tables/{row['Symbol']}/{row['Symbol']}-cam-tables.csv", f='csv')

            # save the pickled Camelot <TablesList> object (it can be used to export to new types)
            pathlib.Path(f"camelot-TablesList-objects/")  # necessary?
            picklefile = open(f'{self.camelot_table_objects[row["Symbol"]]}-camelot-pickled-tableslist-obj', 'wb')
            pickle.dump(self.camelot_table_objects[row["Symbol"]], picklefile)
            picklefile.close()

            return 'Camelot Tables saved and pickled'

        except Exception as e:  # I got this EOF marker not found error and I want to catch it for later
            self.camelot_table_objects[row["Symbol"]] = e.message
            return 'No Camelot Tables'


# this only exists to focus on which pages are required which I'm gna do later
# data["Total Revenue Annual Report Pages"] = data.apply(lambda row: set_pdf_page_numbers(row), axis=1)


if __name__ == '__main__':
    reduced_data_df = pd.read_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv")[:2]
    reduced_data_df.to_csv("reduced_full_data.csv")
    # camelot_table_scraper = CamelotTableScraper(pd.read_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv"))
    # camelot_table_scraper = CamelotTableScraper(pd.read_csv("reduced_full_data.csv"))
    # tables = camelot.read_pdf("tests/annual reports/23076_Whitbread_AR2020_web.pdf", pages=[150], flavor='stream')
