import os
from typing import Optional
from pandas import DataFrame as pdDF 
from sqlalchemy import create_engine
from invoice_connection_details import WK0_BATCH, WK1_BATCH, WK2_BATCH, WK3_BATCH, WK4_BATCH
from invoice_classes import Pencil, DfToPDF, InvoiceWeek, THIS_WEEK

'''to change weeks, simply change the value of THIS_WEEK_NUM in invoice_connection_details!'''

#JOB ACTIONS
def PREPARE_CLASSES(make_new:Optional[bool]=True):
    '''generate a tuple of outputs of len 3 (2 classes and 1 conn str)'''
    from invoice_connection_details import conn_type, username, pwd, hostname, port_id, db_name
    
    def _define_pg_connection(dialect_and_driver, username, password, host, port, db_name) -> str:
        connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
        return connection_string
    
    def generate_pencil(eng, make_new:bool):
        return Pencil(engine=eng, 
                      connection=_define_pg_connection(conn_type, username, pwd, 
                                                       hostname, port_id, db_name), 
                      new_invoice=make_new)

    connection = _define_pg_connection(conn_type, username, pwd, hostname, port_id, db_name)
    engine = create_engine(connection, echo=True)
    pencil = generate_pencil(engine, make_new)
    pdf = DfToPDF()
    return pencil, pdf

#global essentials
MAKE_NEW = True
PENCIL, PDF = PREPARE_CLASSES(MAKE_NEW)
WEEK = InvoiceWeek()
thisWEEK = WEEK.invoice_context

#job actions based on defined global essentials
def insert_invoice_batch(batch_info:list):
    return PENCIL.insert_batch(batch_info)

def fetch_df(this_week:Optional[str]=thisWEEK):
    df0 = PENCIL.read_df_from_pg(f"SELECT * FROM alexander_patrie_prek_invoice_for_{this_week}")
    df1 = PENCIL._add_total_duration(df0)
    return df1
    
def create_excel_from_df(excel_path:str, df:pdDF):
    return PENCIL.create_excel_from_df(excel_path, df)

def create_pdf_from_df(df:pdDF, out_filename:str):
    return PDF.dataframe_to_pdf(df, out_filename)

def send_invoice_text(df:pdDF):
    return PENCIL.send_text(df)


#main job action-activation function
def main(new_invoice_batch:(list), week_number:int):
    from os import path as p, mkdir
    project_dir = PENCIL.project_dir
    excel_invoice_path, pdf_invoice_path = PENCIL.__getitem__()[1], PENCIL.__getitem__()[2]
    insert_invoice_batch(batch_info=new_invoice_batch[week_number])
    df0 = fetch_df(thisWEEK)
    create_pdf_from_df(df0, pdf_invoice_path)
    create_excel_from_df(excel_invoice_path, df0)


if __name__ == "__main__":
    '''just run the weeks that you have DONE so far here as args in main()'''
    main(new_invoice_batch=(WK0_BATCH, WK1_BATCH,
                            WK2_BATCH, WK3_BATCH),
        week_number=WEEK.invoice_week)
    print("Thank you for using me. Job done.")





'''
#MAIN ACTIVATION FUNCTION
def main(new_invoice_batch:(list), week_number:int):
    from os import path as p, mkdir
    project_dir = p.dirname(p.dirname(p.abspath(__file__)))
    excel_invoice_path, pdf_invoice_path = PENCIL.__getitem__()[1], PENCIL.__getitem__()[2]
    insert_invoice_batch(pencil=PENCIL, batch_info=new_invoice_batch[week_number])
    df0 = fetch_df(PENCIL)
    create_pdf_from_df(PDF, df0, pdf_invoice_path)
    create_excel_from_df(PENCIL, excel_invoice_path, df0)

    
if __name__ == "__main__":
    #just run the weeks that you have DONE so far here as args in main()
    main(new_invoice_batch=(WK0_BATCH, WK1_BATCH,
                            WK2_BATCH, WK3_BATCH),
        week_number=PENCIL.invoice_week)
    print("Thank you for using me. Job done.")
'''
    
    
    
    