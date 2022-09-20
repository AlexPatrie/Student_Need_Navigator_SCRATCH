import os, sys
from re import T 
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
    trans_df0 = fetch_read_csv(rose, project_dir, -1) #<-Spark DF
    #show_dfs_in_dir(rose, project_dir)
    trans_df1 = add_student_id(trans_df0, rose) #<-Spark DF
    trans_df2 = normalize_numerical_features(trans_df1, rose) #<-Pd DF
    trans_df3 = oneHot_columns(trans_df2, rose)
    trans_df4 = vectorize_text(trans_df3, rose, "keyword_comments")#<-do this for several cols
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
        for dirpath, dirnames, filenames in os.walk(os.listdir(file_dirpath)[9]):
            return RoseSpark(config={}).fetch_read_csv(spark, 
                                                       file_dirpath, 
                                                       filenames[file_index])#<-going to specify -1 for LAST file in dir

'''PREPROCESSING JOBS(origin: fetch_read_csv):
    add_student_id->normalized_numerical_features->oneHot_columns->vectorize_text==>preprocessed data'''

def add_student_id(df:DataFrame, spark:SparkSession):
    '''add a student_id in place of name for preprocessing/drop name'''
    return RoseSpark(config={}).add_student_id(df, spark) 

def normalize_numerical_features(df, spark:SparkSession):
    '''here numerical features are divided by the .max() of their respective cols/drop original'''
    return RoseSpark(config={}).normalize_numerical_features(df, spark)

def oneHot_columns(df, spark:SparkSession):
    '''here we convert categorical feature values into oneHot Vectors'''
    rose = RoseSpark(config={})
    df0 = rose.oneHot_column(df, spark, "instrument")#<-lets call each col individually HERE
    df1 = rose.oneHot_column(df0, spark, "lesson_location")
    df2 = rose.oneHot_column(df1, spark, "contact")
    df3 = rose.oneHot_column(df2, spark, "lesson_day")
    return df3
    
def vectorize_text(df, spark, col):
    '''use NLP to convert text feature values into vectors'''
    return RoseSpark(config={}).vectorize_text(df, spark, col)

"""RUN MAIN FUNCTION"""  
if __name__ == "__main__":
    main_preprocessing(project_dir)