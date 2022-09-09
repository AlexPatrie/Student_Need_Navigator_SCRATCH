import os, sys 
import logging
from time import asctime 

'''This script represents the actions associated with the job of preprocessing'''


"""LOGGING BLOCK"""
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('log4j2')
sys.path.insert(1, project_dir)

#we are currently not in need of a postgres connector!
   
from classes.spark_class import RoseSpark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame 
import pandas as pd 
import typing 

"""MAIN EXECUTABLE FUNCTION THAT HOUSES JOB ACTIONS"""
def main_preprocessing(project_dir:str) -> None:
    conf = open_config(f"{project_dir}/config/config.json")
    rose = spark_start(conf)#<--Spark Cursor
    clean_students_df = extract_df(rose, project_dir, -1) 
    #show_dfs(rose, project_dir)
    rose.stop()


"""CONFIG BLOCK"""
def open_config(file_path: str) -> dict:
    if isinstance(file_path, str):
        return RoseSpark(config={}).open_file(file_path)

def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        return RoseSpark(config={}).spark_start(conf)
    
def spark_stop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None 
    

"""1. READ IN FRESHLY-WRITTEN CSV TO SPARK DF FROM !DEDICATED(explicit)! CLEAN DIR"""
#I like to order args by order of appearance within creation of funcs!
def show_dfs(spark:SparkSession, file_dirpath:str) -> DataFrame:
    if os.path.exists(file_dirpath):
        #here we recursively target the file name as a string and extract it from os.walk
        for dirpath, dirnames, filenames in os.walk(os.listdir(file_dirpath)[10]):
            i = 0 
            while i < len(filenames): 
                RoseSpark(config={}).fetch_read_csv(spark, file_dirpath, filenames[i])
                i += 1

def extract_df(spark:SparkSession, file_dirpath:str, i:int) -> DataFrame:
    if os.path.exists(file_dirpath):
        for dirpath, dirnames, filenames in os.walk(os.listdir(file_dirpath)[10]):
            print(filenames)
            return RoseSpark(config={}).fetch_read_csv(spark, file_dirpath, filenames[i])
        
                
        

"""RUN MAIN FUNCTION"""  

if __name__ == "__main__":
    main_preprocessing(project_dir)