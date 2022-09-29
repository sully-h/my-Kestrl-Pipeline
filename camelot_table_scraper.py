import pickle

import camelot
import pathlib

import numpy as np
import pandas as pd
from pdf_find_pages import pdf_find_pages


# tables = camelot.read_pdf('annual reports/visiomed_group_plan_strategique_veng (3).pdf')
# tables = camelot.read_pdf('annual reports/spatial-annual-report-2021.pdf')

class CamelotTableScraper():
    def __init__(self, **kwargs):  # should master_stocks be a path to CSV file instead?
        # '''generates a column with Camelot <TableList n=n> data which can be exported''' # I decided to keep the df primitive
        """saves tables to CSV files and returns a success message or a failure message"""
        self.kwargs = kwargs
        self.camelot_table_objects = {}

        # accept either dataframe or path as an argument but not both
        self.master_stocks = self.receive_data()

        self.master_stocks.apply(lambda row: self._get_camelot_table(row), axis=1)

    def receive_data(self):
        if self.kwargs.get("dataframe") is None and self.kwargs.get("path") is None:
            raise ValueError("You must pass in one of dataframe or path")
        if self.kwargs.get("dataframe") is not None and self.kwargs.get("path") is not None:
            raise ValueError("You must pass in either the dataframe or the path to a CSV but not both")

        if self.kwargs.get("dataframe") is not None:  # if it's a dataframe that's been passed in - use that
            return self.kwargs.get("dataframe")
        else:
            return pd.read_csv(self.kwargs.get("path"))

    def _get_camelot_table(self, row):
        """saves CSVs and pickle files for each PDF """
        print(f'Reading (Camelot) PDF for {row["Symbol"]}')
        try:  # default camelot settings
            self.camelot_table_objects[row["Symbol"]] = camelot.read_pdf(row["PDF_path"], flavor='stream',
                                                                         pages=row['Rev PDF Page Numbers'])

            # save all the tables to CSVs inside a camelot-tables/ directory
            pathlib.Path(f"camelot-tables/{row['Symbol']}").mkdir(parents=True, exist_ok=True)
            self.camelot_table_objects[row["Symbol"]].export(
                f"camelot-tables/{row['Symbol']}/{row['Symbol']}-cam-tables.csv", f='csv')

            # save the pickled Camelot <TablesList> object (it can be used to export to new types)
            pathlib.Path(f"camelot-TablesList-objects/").mkdir(parents=True, exist_ok=True)  # necessary?
            picklefile = open(f'{self.camelot_table_objects[row["Symbol"]]}-camelot-pickled-tableslist-obj', 'wb')
            pickle.dump(self.camelot_table_objects[row["Symbol"]], picklefile)
            picklefile.close()

            return 'Camelot Tables saved and pickled'

        except Exception as e:  # I got this EOF marker not found error and I want to catch it for later
            self.camelot_table_objects[row["Symbol"]] = print(e) # I want to put the stack trace in camelot_table-objects(
            return 'No Camelot Tables'


# this only exists to focus on which pages are required which I'm gna do later
# data["Total Revenue Annual Report Pages"] = data.apply(lambda row: set_pdf_page_numbers(row), axis=1)


if __name__ == '__main__':
    reduced_data_df = pd.read_csv(
        "intermediate data sources/EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv")[:2]
    current_data_df = pd.read_csv("reduced_full_data_with_Rev_pages.csv")
    c_data_cam_obj = CamelotTableScraper(dataframe=current_data_df)
    #reduced_data_df.to_csv("tests/reduced_full_data.csv") # no need to save again since I copied and pasted the Rev pages data manually
    # camelot_table_scraper = CamelotTableScraper(pd.read_csv("EOD_LSE_merged_filtered_with_URLS_PDF_paths_with_Total_Revenue.csv"))
    # camelot_table_scraper = CamelotTableScraper(pd.read_csv("reduced_full_data.csv"))
    # tables = camelot.read_pdf("tests/annual reports/23076_Whitbread_AR2020_web.pdf", pages=[150], flavor='stream')
