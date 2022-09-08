import json, os, re, sys
import pandas as pd 
from typing import Callable, Optional 
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

class RoseSpark:
    
    def __init__(self, config):
        self.config = config 
######################################################################## 

#CONFIG SECTION  
    def spark_start(self, kwargs:Optional[dict]=None):
        #these are the nested values, calling by KEY name from the need_nav.json
        MASTER = kwargs['spark_conf']['master']
        APP_NAME = kwargs['spark_conf']['app_name']
        LOG_LEVEL = kwargs['log']['level']
        def create_session(master:Optional[str]="local[*]", 
                           app_name:Optional[str]="NeedNavigator") -> SparkSession:
            spark = SparkSession.builder.appName(app_name)\
                                        .master(master)\
                                        .getOrCreate()                                       
            return spark 
        
        def set_logging(spark:SparkSession, log_level:Optional[str]=None) -> None:
            spark.sparkContext.setLogLevel(log_level) \
            if isinstance(log_level, str) else None 
        
        def get_settings(spark:SparkSession) -> None:
            print(f"\n\033[95m{'SPARK OBJECT: '}")
            print(f"\033[94m{spark}\033[1m\n")
        
            print(f"\033[95m{'SPARK CONTEXT: '}")
            print(f"\033[93m{spark.sparkContext.getConf().getAll()}\033[1m\n")
            
        spark = create_session() 
        set_logging(spark, LOG_LEVEL)       
        get_settings(spark)
        return spark 
  
    def open_file(self, file_path: str) -> dict:
        #automatically, this func occurs, same arg params
        def open_json(file_path: str) -> dict:
            #if there is an instance of that arg(fp) as a str, AND THE PATH EXISTS then...
            if isinstance(file_path, str) and os.path.exists(file_path):
                #using the function open(arg(filepath), in read mode) as psydonym 'f', do this:
                with open(file_path, "r") as f:
                    #data = json.load(the result of open())
                    data = json.load(f)
                #return that result as the value "data"
                return data                         
        print(f'\033[91m{file_path} file opened!\033[1m')    
        return (open_json(file_path))
########################################################################  

#LOAD, READ AND STANDARDIZE SCHEMA/DF   
    def pd_to_spark_df(self, spark, sql, conn):
        df1 = pd.read_sql(sql, con=conn)
        df2 = spark.createDataFrame(df1)
        return df2
    
    def clean_df_to_csv(self, df:DataFrame, dir_path:str, file_name:str):
        def make_clean_csv_dir(dir_path:str) -> str:
            '''pass in the desired path of the folder clean_csv as dir_path so
            path=make_clean_csv_dir...!'''
            #this method only occurs within the other method export to csv
            if not os.path.exists(dir_path) and isinstance(dir_path, str):
                '''this will both make a new path and generate a usable str token'''
                os.mkdir(dir_path)
            #regardless, return str
            return dir_path
        
        
            
        def export_to_csv(df, dir_path, file_name):
            #automate the writing of spark df to csv
            '''in this function, pass the str that is dir path, and desired file_name to save '''
            return df.coalesce(1).write.format("csv").option("header", "true")\
                     .save(f"{dir_path}/{file_name}.csv")
        
        #create new dir if not exists and generate dir_path string          
        dir_path = make_clean_csv_dir(dir_path)
        #write df to csv file and export file to clean dir
        return export_to_csv(df, dir_path, file_name)
########################################################################  

#READ DATA FOR PREPROCESSING
    def fetch_read_csv(self, spark:SparkSession, file_dirpath:str, file_name:str) -> DataFrame:
        if os.path.exists(file_dirpath):
            df = spark.read.format('csv').option('header', 'true')\
                        .option('inferSchema', 'true').load(f"{file_name}.csv")
            df.printSchema()
            df.show(2)
            return df 
########################################################################  