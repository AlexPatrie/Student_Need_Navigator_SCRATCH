from typing import Optional
from sqlalchemy.orm import declarative_base, sessionmaker
from invoice_connection_details import THIS_WEEK
import os 
import pandas as pd 



BASE = declarative_base()
TABLE_NAME = f"alexander_patrie_prek_invoice_for_{THIS_WEEK}"

class Invoice(BASE):
    from sqlalchemy import Column, String, Integer
    __tablename__ = TABLE_NAME
    SERVICE_ID = Column(Integer(), primary_key=True)
    SERVICE_DATE = Column(String())
    SERVICE_RENDERED = Column(String())
    DURATION = Column(Integer())
    
    
#################################END_INVOICE_CLASS###############################
    
    
class Pencil(Invoice):
    
    from sqlalchemy import create_engine

    def __init__(self, engine:create_engine, connection:str, table_name:Optional[str]=TABLE_NAME,
                 base:Optional[declarative_base]=BASE, new_invoice:Optional[bool]=True):
        '''MUST SPECIFY TABLE_NAME!'''
        session = sessionmaker(engine)
        self.local_session = session(bind=engine)
        self.connection = connection 
        self.table_name = table_name
        if new_invoice:
           base.metadata.create_all(engine)
        '''add dedicated dir for pdf, excel, and txt file!!'''
    
    def prompt_batch_insert(self) -> list:         
        '''method that prompts user to add info to batch_list'''   
        batch = []
        amt_to_enter = int(input("How many services would you like to add?: "))
        if amt_to_enter > 0:
            for amt in range(amt_to_enter):
                service_date = input("service_date(mm/dd/yyyy): ")
                service_rendered = input("service rendered: ")
                duration = int(input("service_duration: "))
                new_invoice_info = (service_date,
                                    service_rendered,
                                    duration)
                batch.append(new_invoice_info)
        return batch
    
    def insert_batch(self, batch_info):
        '''insert n_dim batch into the db'''
        for row in batch_info:
            new_invoice = Invoice(SERVICE_ID=row[0],
                                  SERVICE_DATE=row[1],
                                  SERVICE_RENDERED=row[2], 
                                  DURATION=row[3])
            self.local_session.add(new_invoice)
            self.local_session.commit()
            print('\n***new invoice added!!!***\n')
        
    def prompt_create_invoice(self, write_batch:Optional[bool]=False):
        '''will prompt the user for the amount of entries that they want to enter into
           the db'''
        batch = self.prompt_batch_insert()
        if write_batch:
            self.write_invoice_info_file(batch)
        return self.insert_batch(batch)
                
    def refresh_output_files(self, txt_file_path, excel_file_path):
        if os.path.exists(txt_file_path) and os.path.exists(excel_file_path):
            os.remove(txt_file_path)
            os.remove(excel_file_path)
            
    def read_df_from_pg(self, sql) -> pd.DataFrame:
        return pd.read_sql(sql, self.connection)
    
    def create_excel_from_df(self, excel_path:str, df:pd.DataFrame):
        df.to_excel(excel_path,  
                    header=True)
    
     
    def _add_total_duration(self, df:pd.DataFrame):
        df["TOTAL_WEEK_DURATION"] = df["DURATION"].sum()
        df.iloc[1:4, 4] = ""
        return df 
        

##############################END_PENCIL_CLASS####################################


import numpy as np
import matplotlib.pyplot as plt
from invoice_connection_details import THIS_WEEK
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties


class DfToPDF():
    '''inspired by towarddatascience.com'''
    def _draw_as_table(self, df, pagesize):
        alternating_colors = [['white'] * len(df.columns), ['peachpuff'] * len(df.columns)] * len(df)
        alternating_colors = alternating_colors[:len(df)]
        fig, ax = plt.subplots(figsize=pagesize)
        ax.axis('tight')
        ax.axis('off')
        space = "\n" * 10
        title = f"~Alexander Patrie~\n\nInvoice for the week of:\n{THIS_WEEK[8:].replace('_', '/')}"
        fig.suptitle(title, ha="center", fontsize=20, 
                     bbox={"pad":0.7, "boxstyle":"square", "facecolor":"peachpuff"}, y=0.70, 
                     fontweight="bold")
        the_table = ax.table(cellText=df.values,
                             colLabels=df.columns,
                             rowLabels=range(len(df)),
                             rowColours=['lightskyblue']*len(df),
                             colColours=['lightskyblue']*len(df.columns),
                             cellColours=alternating_colors,
                             loc='center')
        for (row, col), cell in the_table.get_celld().items():
            if (row != 0):
                cell.set_text_props(fontproperties=FontProperties(weight="bold"))
        the_table.set_fontsize(50)
        the_table.scale(1, 4)
        fig.subplots_adjust(top=0.8)
        return fig
  
    def dataframe_to_pdf(self, df, out_filename, numpages=(1,1), pagesize=(20, 14)):
        with PdfPages(out_filename) as pdf:
            nh, nv = numpages
            rows_per_page = len(df) // nh
            cols_per_page = len(df.columns) // nv
            for i in range(0, nh):
                for j in range(0, nv):
                    page = df.iloc[(i*rows_per_page):min((i+1)*rows_per_page, len(df)),
                                (j*cols_per_page):min((j+1)*cols_per_page, len(df.columns))]
                    fig = self._draw_as_table(page, pagesize)
                    if nh > 1 or nv > 1:
                        # Add a part/page number at bottom-center of page
                        fig.text(0.5, 0.5/pagesize[0],
                                "Part-{}x{}: Page-{}".format(i+1, j+1, i*nv + j + 1),
                                ha='center', fontsize=8)
                    pdf.savefig(fig, bbox_inches='tight')
                    print("\nPDF of Invoice created!\n")
                    plt.close()
    

####################################END_PDF_CLASS####################################
