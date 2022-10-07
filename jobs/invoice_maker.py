from typing import Optional
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, DateTime, Integer, Float, create_engine 
from datetime import datetime
import invoice_connection_details as icd 
import sys 
import os 

import logging
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame 


def define_connection(dialect_and_driver, username, password, host, port, db_name:Optional[str]=icd.september_22_db):
    connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
    return connection_string

CONNECTION = define_connection(icd.conn_type, icd.username, icd.pwd, 
                               icd.hostname, icd.port_id, icd.september_22_db)
BASE = declarative_base()
ENGINE = create_engine(CONNECTION, echo=True)
SESSION = sessionmaker(ENGINE)

#logging block
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('log4j2')
sys.path.insert(1, project_dir)

from spark_class import RoseSpark


class Invoice(BASE):
    '''we also want a col that is total duration that is a sum of duration...'''
    __tablename__ = f"alexander_patrie_prek_invoice_for_{icd.THIS_WEEK}"
    service_date = Column(String(), primary_key=True)
    service_rendered = Column(String())
    duration = Column(Integer())
    
class Pencil(Invoice):
    def __init__(self):
        self.local_session = SESSION(bind=ENGINE)
        BASE.metadata.create_all(ENGINE)
        
    def create_invoice(self):
        def prompt_batch_insert(batch=[]):         
            '''method that prompts user to add info to batch_list'''   
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
        
        def insert_batch(batch_info):
            row = 0
            while row < len(batch_info):
                new_invoice = Invoice(service_date=batch_info[row][0],
                                      service_rendered=batch_info[row][1], 
                                      duration=batch_info[row][2])
                self.local_session.add(new_invoice)
                self.local_session.commit()
                #visually printing confirmation for my own sanity!
                print('\n***new invoice added!!!***\n')
                row += 1
        
        invoice = prompt_batch_insert()
        return insert_batch(invoice)


def open_config(file_path: str) -> dict:
    if isinstance(file_path, str):
        return RoseSpark(config={}).open_file(file_path)

def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        return RoseSpark(config={}).spark_start(conf)
    
def spark_stop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None
    
def read_in_from_pg(spark, sql, conn):
    return RoseSpark(config={}).read_pd_to_spark_df(spark, sql, conn)
            
        
if __name__ == "__main__":
    conf = open_config(f"{project_dir}/config/config.json")
    rose = spark_start(conf)
    pencil = Pencil()
    pencil.create_invoice()
    df0 = read_in_from_pg(rose, 
                          f"SELECT * FROM alexander_patrie_prek_invoice_for_{icd.THIS_WEEK}",
                          CONNECTION)
    print(df0)
    
    