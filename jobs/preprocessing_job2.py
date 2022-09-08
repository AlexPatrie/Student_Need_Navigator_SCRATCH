import os, sys 
import logging
from time import asctime 

'''This script represents the actions associated with the job of preprocessing'''


"""LOGGING BLOCK"""
#this is the FULL dir to the project, including nested folders, ending with THIS path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#file path format for creating new logs...this project @whatever the name of the file is.log
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
#the format of the actual log, with params specfied
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
#creating a config for the logging based on our strings
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
#specifying the logger(jvm)
logger = logging.getLogger('log4j2')
#here we are allowing this path to be connected alongside classes
sys.path.insert(1, project_dir)

#we are currently not in need of a postgres connector!
   
from classes.spark_class import RoseSpark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame 
import pandas as pd 
import typing 

"""MAIN EXECUTABLE FUNCTION THAT HOUSES JOB ACTIONS"""
def main(project_dir:str) -> None:
    conf = open_config(f"{project_dir}/config/config.json")
    rose = spark_start(conf)#<--Spark Cursor
    fetch_read_csv(rose, 
                   "/Users/alex/Desktop/Student_Need_Navigator_SCRATCH/clean_csv/",
                   "cleaned_students")


"""CONFIG BLOCK"""
def open_config(file_path: str) -> dict:
    if isinstance(file_path, str):
        return RoseSpark(config={}).open_file(file_path)

def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        return RoseSpark(config={}).sparkStart(conf)
    
def spark_stop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None 
    
"""1. READ IN FRESHLY-WRITTEN CSV FROM !DEDICATED! CLEAN DIR"""
def fetch_read_csv(spark:SparkSession, file_dirpath:str, file_name:str) -> DataFrame:
    if os.path.exists(file_dirpath):
        df = spark.read.format('csv').option('header', 'true')\
                       .option('inferSchema', 'true').load(f"{file_name}.csv")
        df.printSchema()
        df.show(2)
        return df 
    