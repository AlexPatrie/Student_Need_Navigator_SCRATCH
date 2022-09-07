import json, os, re, sys
from typing import Callable, Optional 
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

class RoseSpark:
    
    def __init__(self, config):
        self.config = config 
        
    def sparkStart(self, kwargs:Optional[dict]=None):
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
    
    
    def make_clean_csv_dir(self, dir_path:str) -> str:
        '''pass in the desired path of the folder clean_csv as dir_path so
           path=make_clean_csv_dir...!'''
           #this method only occurs within the other method export to csv
        if not os.path.exists(dir_path) and isinstance(dir_path, str):
            '''this will both make a new path and generate a usable str token'''
            os.mkdir(dir_path)
            return dir_path
        
    def export_to_csv(self, df, dir_path, file_name):
        '''in this function, pass the '''
        file_path = self.make_clean_csv_dir(dir_path)
        return df.coalesce(1).write.format("csv").option("header", "true")\
                 .save(f"{file_path}/{file_name}.csv")