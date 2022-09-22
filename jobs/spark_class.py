import json, os, re, sys
import pandas as pd 
from typing import Callable, Optional 
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col



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
        def open_json(file_path: str) -> dict:
            if isinstance(file_path, str) and os.path.exists(file_path):
                with open(file_path, "r") as f:
                    data = json.load(f)
                return data                         
        print(f'\033[91m{file_path} file opened!\033[1m')    
        return (open_json(file_path))
    
    '''method used to create an f string containing the sqlalchemy conn string
    required to connect to postgress via sqlalchemy.create_engine()'''
    def define_pg_connection(self, dialect_and_driver, username, password, host, port, db_name:Optional[str]='students_sept_2022'):
        connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
        return connection_string
########################################################################  

#LOAD, READ AND STANDARDIZE SCHEMA/DF
    #read in df with sql and pandas and then convert to spark df(get around jdbc issue)    
    def read_pd_to_spark_df(self, spark, sql, conn):
        df1 = pd.read_sql(sql, con=conn)
        df2 = spark.createDataFrame(df1)
        return df2
        
########################################################################   
    #implemented the use of a bool to determine if the class is writing a csv via spark or pd
    def clean_df_to_csv(self, df:DataFrame, project_dir:str, file_name:str, is_spark:bool):
        def pd_2_csv(df, file_name):
            df1 = df.toPandas()
            os.makedirs(f'{project_dir}/clean_csv', exist_ok=True)  
            df2 = df1.to_csv(f'{project_dir}/clean_csv/{file_name}.csv')  
            print("Pandas df created!")
            return df2
            
        return df.coalesce(1).write.format("csv")\
                 .option("header", "true").save(file_name) if is_spark == True\
                 else pd_2_csv(df, file_name) 
                 
########################################################################                   
    def clean_file_names(self, project_dir, file_name): 
        def generate_raw_file_name(project_dir:str, file_name:str) -> str:
            return os.listdir(f"{project_dir}/{file_name}")[0]

        def rename_files(old_file_name, new_file_name):       
            try:
                os.rename(old_file_name, new_file_name)
            except FileExistsError:
                print("File already Exists")
                print("Removing existing file")
                os.remove(old_file_name)
                os.rename(old_file_name, new_file_name)
        
        raw_file_name = generate_raw_file_name(project_dir, file_name)
        print(str(raw_file_name))
        return rename_files(raw_file_name, "clean_student_df.csv")
    
########################################################################    

#PREPROCESSING METHODS

    #read single file for preprocessing
    def fetch_read_csv(self, spark:SparkSession, file_dirpath:str, file_name:str) -> DataFrame:
        if os.path.exists(file_dirpath):
            df0 = spark.read.format('csv').option('header', 'true')\
                            .option('inferSchema', 'true').load(f"{file_dirpath}/clean_csv/{file_name}")
            df1 = df0.drop(df0.columns[0])#<-drop col '_c0' that is created when spark reads a csv(0th index)
            return df1 
        else:
            print('path no exist!')
        
    #we are going to use pyspark .lit() to insert new col with literals    
    def add_student_id(self, df:DataFrame, spark:SparkSession):
        def create_id(df):
            from pyspark.sql.functions import monotonically_increasing_id 
            df0 = df.select("*").withColumn("student_id", monotonically_increasing_id())
            return df0
        
        '''I want to rearrange the order of the cols so that the target(y aka Label)
           is at position -1'''
        def rearrange_cols(df, spark):
            df.createOrReplaceTempView("d")
            df0 = spark\
                    .sql("select student_id, instrument, lesson_location, \
                                 contact, time_of_lesson, lesson_day, \
                                 age, keyword_comments, status, lesson_material  \
                          from d")
            return df0 
        
        df0 = create_id(df)
        return rearrange_cols(df0, spark)
        
    def normalize_numerical_features(self, df, spark):
        #convert to pandas for normalization
        df0 = df.toPandas()
        #create normalized cols (colValue/colValue.max())
        df0['normalized_time_of_lesson'] = df0['time_of_lesson'] / df0['time_of_lesson'].max()
        df0['normalized_age'] = df0['age'] / df0['age'].max()
        df0['normalized_status'] = df0['status'] / df0['status'].max()
        #drop original unnormalized cols
        df1 = df0.drop(['time_of_lesson', 'age', 'status'], axis=1)
        #map out replacement for specified character in specified column so that there are only NON-zero values
        df1['normalized_status'].replace({0.0: 0.1}, inplace=True)
        df2 = spark.createDataFrame(df1)
        return df2

    def oneHot_column(self, df, spark, column:str): #<-column is that which will be oneHotted
        from pyspark.ml.feature import OneHotEncoder, StringIndexer 
        df.createOrReplaceTempView("d")
        df1 = spark.sql("select * from d") #<-includes ALL cols
        indexer = StringIndexer(inputCol=f"{column}", outputCol=f"{column}_index")
        df2 = indexer.fit(df1).transform(df1)
        ohe = OneHotEncoder(inputCol=f"{column}_index", outputCol=f"{column}_ohv")
        df3 = ohe.fit(df2).transform(df2)
        df4 = df3.drop(f"{column}", f"{column}_index")  
        #make sure to rearragnge cols with feature at the end!
        return df4
        
    def vectorize_text(self, df, spark, column):
        import pyspark.sql.functions as f
        from pyspark.ml.feature import RegexTokenizer
    
        def clean_None_vals(df):
            df0 = df.toPandas()
            df1 = df0.replace({None: 'no entry added'})
            return df1
        
        def tokenize(df, column):
            df0 = spark.createDataFrame(df)
            
            tokenizer = RegexTokenizer(outputCol=f"{column}_tokenized")
            tokenizer.setInputCol(column)
            df1 = tokenizer.transform(df0)
            return df1
        
        '''outputs a pandas df with vectorized column!'''
        def add_str_index(column, df): 
            #...these are the tokenized keyword comments in 2d array
            df.createOrReplaceTempView("TAB")
            df1 = spark.sql(f"SELECT DISTINCT {column}_tokenized FROM TAB")
            #raw list with duplicate values
            term_list_raw = df.select(f.collect_list(f"{column}_tokenized")).first()[0]
            ##below is a python-list of a df that has all the unique vals
            term_list = df1.select(f.collect_list(f"{column}_tokenized")).first()[0]
            #flatten the list without having to use numpy
            flat_list = [item for sublist in term_list for item in sublist]
            #map as a dictionary...format => {term:index}
            map2 = dict([(y,x+1) for x,y in enumerate(sorted(set(flat_list)))])
            
            '''#1'''
            #matches the df col values with the values from map and extracts
            def get_vector(term_list, map2, i):
                y = []
                for term in term_list[i]:
                    y.append(map2[term])
                    i += 1
                return y
            
            '''#2'''
            #returns index vectors of given col as a list!!
            def realize_list(term_list_raw):
                y = []
                for n in range(len(term_list_raw)):
                    y.append(get_vector(term_list_raw, map2, n))
                return y 
                
            term_vectors = realize_list(term_list_raw)#<-2d list of vectorized str terms for given col
            df2 = df.toPandas()
            df2[f"{column}_vect"] = term_vectors
            return df2
            ###END STR INDEXER###
                 
        df0 = clean_None_vals(df)
        df1 = tokenize(df0, column)
        df2 = add_str_index(column, df1)#<-pandas df with new column
        #drop irrelevant
        df3 = df2.drop([column, f"{column}_tokenized"], axis=1)
        #normalize string indicies between 0-1
        df3[f"normalized_{column}_vect"] = df3[f"{column}_vect"] / df3[f"{column}_vect"].max()
        #drop irrelevent
        df4 = df3.drop([f"{column}_vect"])
        #convert to spark for reiteration (spark in -> spark out)
        df5 = spark.createDataFrame(df4)
        return df5
        ###END VECTORIZE TEXT############
        
        