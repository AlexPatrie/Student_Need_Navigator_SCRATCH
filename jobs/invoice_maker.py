from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Integer, create_engine, ForeignKey 
from datetime import datetime
import invoice_connection_details as icd 
import os 
from invoice_classes import Pencil, DfToPDF
from pandas import DataFrame as pdDF, read_sql 



   
'''JOB ACTIONS'''
def insert_invoice_batch(pencil:Pencil, batch_info:list):
    return pencil.insert_batch(batch_info)


def fetch_df(pencil:Pencil):
    df0 = pencil.read_df_from_pg(f"SELECT * FROM alexander_patrie_prek_invoice_for_{icd.THIS_WEEK}")
    df1 = pencil._add_total_duration(df0)
    return df1
    
    
def create_excel_from_df(pencil:Pencil, excel_path:str, df:pdDF):
    return pencil.create_excel_from_df(excel_path, df)


def create_pdf_from_df(pdf:DfToPDF, df:pdDF, out_filename:str):
    return pdf.dataframe_to_pdf(df, out_filename)





'''MAIN ACTIVATION FUNCTION'''
def main(new_invoice_batch:list):
    
    def _define_pg_connection(dialect_and_driver, username, password, host, port, db_name) -> str:
        connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
        return connection_string
    
    from invoice_connection_details import conn_type, username, pwd, hostname, port_id, db_name, THIS_WEEK
    CONNECTION = _define_pg_connection(conn_type, username, pwd, hostname, port_id, db_name)
    ENGINE = create_engine(CONNECTION, echo=True)
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INVOICE_DIR_PATH = f"{PROJECT_DIR}/invoice_data_PREK_2022"
    if not os.path.exists(INVOICE_DIR_PATH):
        os.mkdir(INVOICE_DIR_PATH)
    INVOICE_EXCEL_PATH = f"{INVOICE_DIR_PATH}/{THIS_WEEK}.xls"
    INVOICE_PDF_PATH = f"{INVOICE_DIR_PATH}/invoice_{THIS_WEEK}.pdf"
    
    PEN = Pencil(engine=ENGINE,  
                 connection=CONNECTION)
    PDF = DfToPDF()
    
    insert_invoice_batch(pencil=PEN, batch_info=new_invoice_batch)
    df0 = fetch_df(PEN)
    create_excel_from_df(PEN, INVOICE_EXCEL_PATH, df0)
    create_pdf_from_df(PDF, df0, INVOICE_PDF_PATH)

if __name__ == "__main__":
    WK0_BATCH = [[0, "10/03/2022", "Music Class - PREK", 30],
                 [1, "10/03/2022", "Music Class - TODDLER", 30],
                 [2, "10/06/2022", "Music Class - PREK", 30],
                 [3, "10/06/2022", "Music Class - TODDLER", 30]]
    main(WK0_BATCH)
    
    
    
    