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
   
from spark_class import RoseSpark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame 
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import OneHotEncoder, StringIndexer, Tokenizer, Word2Vec


"""MAIN EXECUTABLE FUNCTION THAT HOUSES JOB ACTIONS"""
def main_preprocessing(project_dir:str) -> None:
    conf = open_config(f"{project_dir}/config/config.json")
    rose = spark_start(conf)#<--Spark Cursor
    clean_df0 = fetch_read_csv(rose, project_dir, -1)
    #show_dfs_in_dir(rose, project_dir)
    clean_df1 = add_student_id(clean_df0, rose)
    clean_df1.printSchema()
    normalize_numerical_features(clean_df1, rose)
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
def show_dfs_in_dir(spark:SparkSession, file_dirpath:str) -> DataFrame:
    if os.path.exists(file_dirpath):
        #here we recursively target the file name as a string and extract it from os.walk
        for dirpath, dirnames, filenames in os.walk(os.listdir(file_dirpath)[10]):
            i = 0 
            while i < len(filenames): 
                RoseSpark(config={}).fetch_read_csv(spark, file_dirpath, filenames[i])
                i += 1

#I wanted to be explicit about the naming of arg "file_index" so that it is not ambiguous like "i"
def fetch_read_csv(spark:SparkSession, file_dirpath:str, file_index:int) -> DataFrame:
    if os.path.exists(file_dirpath):
        #here we iterate over the dedicated csv dir and specify the index position(10)
        for dirpath, dirnames, filenames in os.walk(os.listdir(file_dirpath)[8]):
            return RoseSpark(config={}).fetch_read_csv(spark, 
                                                       file_dirpath, 
                                                       filenames[file_index])#<-going to specify -1 for LAST file in dir

#I would like to add a col 'student_id' for preprocessing and drop name col
def add_student_id(df:DataFrame, spark:SparkSession):
    return RoseSpark(config={}).add_student_id(df, spark) 
       
"""ADD PREPROCESSING JOBS NOW:
    we must vectorize categorical data and normalize numerical data"""                

def normalize_numerical_features(df:DataFrame, spark:SparkSession):
    return RoseSpark(config={}).normalize_numerical_features(df, spark)




"""RUN MAIN FUNCTION"""  
if __name__ == "__main__":
    main_preprocessing(project_dir)