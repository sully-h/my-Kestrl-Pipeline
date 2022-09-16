import pandas as pd
from parsr_client import ParsrClient, ParsrOutputInterpreter

def parsr_table_maker(stocks : pd.DataFrame):
    '''call the Parsr Python client and return CSV'''
    pass

parsr = ParsrClient('localhost:3001')

parsr.send_document(
    file_path='annual-reports/AADV_annual_report.pdf',
    config_path='defaultConfig.json',
    document_name='AADV Annual Report',
    save_request_id=True
)

