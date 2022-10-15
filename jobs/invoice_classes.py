import os 
import pandas as pd  
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from invoice_connection_details import OCT22_inv_wk_strt_days
from typing import Optional
from invoice_connection_details import JOB_CONTEXT, THIS_WEEK_NUM


class InvoiceWeek():
    def __init__(self, 
                 invoice_week:Optional[int]=THIS_WEEK_NUM, 
                 inv_week_strt_days:Optional[list]=OCT22_inv_wk_strt_days):
        self.invoice_week = invoice_week
        self.CURRENT_YEAR = str(datetime.now().year)
        self.CURRENT_MONTH = str(datetime.now().month)#<-IF ACCESSING OLD INVOICES, MANUALLY CHANGE THIS VALUE, ELSE AUTO MONTH!
        self.inv_week_start_days = inv_week_strt_days
        self.invoice_context = self.__getitem__(invoice_week)
    
    def __getitem__(self, week_num):
        return self._generate_invoice_context(week_num, self.inv_week_start_days)
        
    def _generate_invoice_context(self, week_num:int, start_days:Optional[list]=OCT22_inv_wk_strt_days):
        def get_week(month, start_days:list, week_num:int, year):
            return f"week_of_{month}_{start_days[week_num]}_{year}" 
        return get_week(self.CURRENT_MONTH, start_days, week_num, self.CURRENT_YEAR)
    
    def _generate_table_context(self, place_of_business:str):
        this_week = self.invoice_context
        table_name = f"alexander_patrie_{place_of_business}_invoice_for_{this_week}"
        return this_week, table_name 
    
#####################################END_WEEK_NUM_CLASS#####################################

iw = InvoiceWeek()
THIS_WEEK, TABLE_NAME = iw._generate_table_context(JOB_CONTEXT)
BASE=declarative_base()

class Invoice(BASE):
    from sqlalchemy import Column, String, Integer
    __tablename__ = TABLE_NAME
    SERVICE_ID = Column(Integer(), primary_key=True)
    SERVICE_DATE = Column(String())
    SERVICE_RENDERED = Column(String())
    DURATION = Column(Integer())
    
#################################END_INVOICE_CLASS###############################


class Pencil(Invoice, InvoiceWeek):
    from sqlalchemy import create_engine
    import datetime
    __current_year = str(datetime.datetime.now().year)
    
    def __init__(self, 
                 engine:create_engine, 
                 connection:str,  
                 base:Optional[declarative_base]=BASE, 
                 this_week:Optional[str]=THIS_WEEK,
                 year:Optional[str]=__current_year,
                 new_invoice:Optional[bool]=True):
        '''ONE MUST specify the connection and engine when instantiating this class.
           Think of the connection as the gas that powers the engine. Engine and 
           connection work together to create a session and thus provide a landscape
           by which the cursor(which is this class) can drill into and traverse the 
           postgrSQL server and database(s)ß'''
        super().__init__()
        session = sessionmaker(engine)
        self.local_session = session(bind=engine)
        self.connection = connection 
        self.year = year 
        self.this_week = this_week
        self.project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.invoice_data_dir = self.__getitem__()[0]
        if new_invoice is not False:
           base.metadata.create_all(engine)
        if not os.path.exists(self.invoice_data_dir):
            os.mkdir(self.invoice_data_dir)
        '''add dedicated dir for pdf, excel, and txt file!!'''
        
        
    def __getitem__(self):#, file_names:list):
        def _get_dir_path() -> str:
            _INVOICE_DIR_NAME = f"invoice_data_NMS_PREK_{self.year}"
            return os.path.join(self.project_dir, _INVOICE_DIR_NAME)
        
        def _get_dirFiles_of_type(file_type:str) -> list:
            '''get certian files in the data dir'''
            file_list = []
            for path, _, files in os.walk(_get_dir_path()):
                for f in files:
                    if f[-3:] == file_type:
                        file_path = os.path.join(path, f)
                        file_list.append(file_path)
            return file_list
        
        def _get_data_file_path(file_name:str):
            return os.path.join(_get_dir_path(), file_name) 
        
        data_dir_path = _get_dir_path()
        excel_invoice_path = _get_data_file_path(f"{self.this_week}.xls")
        pdf_invoice_path = _get_data_file_path(f"{self.this_week}.pdf")
        return data_dir_path, excel_invoice_path, pdf_invoice_path
        
        
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
        '''will prompt the user for the amount of entries that they 
           want to enter into the db'''
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
        
        
    from invoice_connection_details import _REC_PHONE
    def send_text(self, df:pd.DataFrame, 
                  txt_file_name:Optional[str]=f"{THIS_WEEK}_invoice_data.txt",
                  rec_phone:Optional[str]=_REC_PHONE):
        '''here the invoices will be sent via text message and saved as a text file
           alongside the excel and pdf copy'''
        from invoice_connection_details import _TM_TOKEN, _TM_UNAME
        from textmagic.rest import TextmagicRestClient
        username = _TM_UNAME
        token = _TM_TOKEN
        client = TextmagicRestClient(username, token)
        
        def extract_str(df):
            data_str = " "
            data_list = [x.tolist() for x in df.values]
            for list in data_list:
                for item in list:
                    item = os.path.join(str(item), ", ")
                    data_str = data_str + str(item)
            return data_str.replace("/", "")
        
        def df_to_map_str(df):
            keys = df['SERVICE_ID'].values.tolist()
            values = df.drop(['SERVICE_ID'], axis=1).values.tolist()
            map0 = dict(zip(keys, values))
            map1 = str(map0)
            
            def save_txt(map_string):
                with open(os.path.join(self.invoice_data_dir, txt_file_name), "w") as f:
                    f.write(map_string)
                    
            save_txt(map1)
            return map1 
        
        data_map_str = df_to_map_str(df)
        print(f"A TEXT FILE CALLED {txt_file_name} WAS CREATED IN {self.invoice_data_dir}")
        return client.messages.create(phones=rec_phone, text=data_map_str)
        
##############################END_PENCIL_CLASS####################################


import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties

class DfToPDF():
    '''eventually, please customize this class with self.attributes such that you 
       change the color and layout/format of the pdf on the fly based on the season!
       if time=winter, make blue, if time=spring, make green, etc. Customizable!'''
    
    def __init__(self, 
                 this_week:Optional[str]=THIS_WEEK):
        '''this_week may be changed to reflect and access an archived invoice...
           if accessing data from a time other than the current week, please specify the week
            attribute up instantiation in the following format: 
            week_of_MONTH_INVOICE_WEEK_START_DAY_YEAR'''
        self.this_week = this_week
    
    
    #inspired by towarddatascience.com
    def _draw_as_table(self, df, pagesize):
        alternating_colors = [['white'] * len(df.columns), ['peachpuff'] * len(df.columns)] * len(df)
        alternating_colors = alternating_colors[:len(df)]
        fig, ax = plt.subplots(figsize=pagesize)
        ax.axis('tight')
        ax.axis('off')
        space = "\n" * 10
        title = f"~Alexander Patrie~\n\nInvoice for the week of:\n{self.this_week[8:].replace('_', '/')}"
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
