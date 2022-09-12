import json, os, re, sys
import logging 
from time import asctime 
from typing import Callable, Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col 
import seaborn as sns 
import matplotlib.pyplot as plt
import pandas as pd 
 


'''This script represents a series of jobs that the class will execute. These
jobs are particularly for the general trimming and standarizing of dtypes. This
has nothing to do with the preprocessing job script '''

#logging block
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('log4j2')
sys.path.insert(1, project_dir)

from spark_class import RoseSpark

def main_data_intake(project_dir:str) -> None:
    from connectionDetails import THIS_WEEK
    
    #CONNECTION BLOCK
    #this executes the func below, which requires the following formatted string(.json'''
    conf = open_config(f"{project_dir}/config/config.json")
    #this applies the string(config) to spark start
    rose = spark_start(conf)
    #params that disallow the creation of multiple files within spark.write...
    rose.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    rose.conf.set("parquet.enable.summary-metadata", "false")
    rose.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    #create pg connection string
    connection = define_pg_conn()
    
    #use pd to read in sql table and convert to spark df!
    df = read_pd_to_spark_df(rose, f"SELECT * FROM students_{THIS_WEEK}", connection)
    
    #TRANSFORMATION BLOCK
    #use job func to remove null and standardize schema
    clean_df = clean_student_data(rose, df) 
    #here is where we write our df to a csv into a dedicated dir that is created
    clean_df_to_csv(df=clean_df, project_dir=project_dir, file_name=f"cleaned_students{THIS_WEEK}", is_spark=False)#<-filename to be written
    clean_df_to_csv(df=clean_df, project_dir=project_dir, file_name=f"cleaned_students_test", is_spark=False)
    #clean_csv_name(project_dir, "cleaned_students")
    
    rose.stop()


'''JOB-SPECIFIC ACTIONS'''
def open_config(file_path: str) -> dict:
    if isinstance(file_path, str):
        return RoseSpark(config={}).open_file(file_path)

def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        return RoseSpark(config={}).spark_start(conf)
    
def spark_stop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None 
    
def define_pg_conn() -> str:
    import connectionDetails as cd 
    return RoseSpark(config={}).define_pg_connection(cd.conn_type, cd.username, cd.pwd, 
                                                     cd.hostname, cd.port_id, cd.db_name)

def read_pd_to_spark_df(spark, sql, conn):
    '''read in a "raw" dataset as a spark df via Pandas'''
    return RoseSpark(config={}).read_pd_to_spark_df(spark, sql, conn)

#here we have a function that cleans null and casts correct dtypes with spark
def clean_student_data(spark, df):
  #replace null values in different cols with spark
  def replace_null(spark, df) -> DataFrame: 
    df.createOrReplaceTempView("d1")
    df1 = spark.sql("SELECT *, CASE WHEN contact_info IS NULL OR contact_info = 'NaN' THEN 'contact thru store' else contact_info end as contact FROM d1;")
    df2 = df1.drop(col("contact_info"))
    df2.createOrReplaceTempView("d2")
    df3 = spark.sql("select *, case when lesson_time like 'NULL' OR lesson_time = 'NaN' then 440 else lesson_time end as time from d2")
    df4 = df3.drop(col("lesson_time"))
    df4.createOrReplaceTempView("d3")
    df5 = spark.sql("select *, case when day_of_study IS NULL OR day_of_study = 'NaN' then 'Thursday' else day_of_study end as lesson_day from d3")\
              .drop(col("day_of_study"))
    df5.createOrReplaceTempView("d4")
    df6 = spark.sql("select *, case when age like 'NULL' OR age = 'NaN' then 14.6 else age end as student_age from d4")\
              .drop(col("age"))
    return df6
    
  def standardize_schema(df):
    #standardize dtypes in schema and then drop copys of columns 
    df0 = df.select("*")\
            .withColumn("time_of_lesson", col("time").cast("Double"))\
            .withColumn("age", col("student_age").cast("Double"))\
            .drop(col("student_age")).drop(col("time"))
    return df0

  df_first = replace_null(spark, df)
  df_final = standardize_schema(df_first)
  return df_final 

#takes the same amount of args and the same name...I want to be as clear as possible!
def clean_df_to_csv(df:DataFrame, project_dir:str, file_name:str, is_spark:bool):
    if isinstance(df, DataFrame):
        return RoseSpark(config={}).clean_df_to_csv(df, project_dir, file_name, is_spark)
    
def clean_csv_name(project_dir, file_name):
    return RoseSpark(config={}).clean_file_names(project_dir, file_name)


if __name__ == '__main__':
    main_data_intake(project_dir)
#export to dedicated dir for sending to another db as a table that will 
    #soon have the model's predictions appended on to it!  




