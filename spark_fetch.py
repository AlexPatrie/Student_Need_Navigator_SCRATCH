'''this script is to serve as simply fetching the data from sql
and turning it into a csv with spark'''

from pyspark.sql import SparkSession, types as T 
import numpy as np
from datetime import datetime
import typing


def main():
    spark = create_session("local[*]", "Need Nav Fetch")
    

def create_session(master:str, app_name:str) -> SparkSession:
    spark = SparkSession.builder\
                        .appName(app_name)\
                        .master(master)\
                        .getOrCreate() 
    print(f"\n\033[96mSpark Session with appName: {app_name} and master: {master} created!\033[1m")   
    return spark 

def transform_sql(spark:SparkSession, db_name: str):
    data = {"headers": ["product_id", "store1", "store2", "store3"], "values": [[0, 95, 100, 105], [1, 70, 0, 80]]}




if __name__ == "__main__":
    main()
                    
                      
                      
                      
#df_types = T.StructType([
#    T.StructField("name", T.StringType(), False),
#    T.StructField("instrument", T.StringType(), False),
#    T.StructField("contact_info", T.StringType()),
#    T.StructField("lesson_time", T.IntegerType()),
#    T.StructField("day_of_study", T.StringType()),
#    T.StructField("age", T.DoubleType()),
#    T.StructField("date_entered", T.DateType()),
#    T.StructField("keyword_comments", T.StringType()),
#    T.StructField("status", T.DoubleType(), False),   
#])                      