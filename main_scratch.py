import pandas as pd 
import jobs.connectionDetails as cd 
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, DateTime, Integer, Float, create_engine 
from datetime import datetime
'''#######################################################'''

'''#method used to create an f string containing the sqlalchemy conn string
    required to connect to postgress via sqlalchemy.create_engine()'''
def define_connection(dialect_and_driver, username, password, host, port, db_name):
    connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
    return connection_string

'''#instance of a correctly formmated sqlalchemy conn string using admin logindetails'''
CONNECTION = define_connection(cd.conn_type, cd.username, cd.pwd, 
                               cd.hostname, cd.port_id, cd.db_name)

'''#that which pertains to a database in sqla'''
Base = declarative_base()
'''#that which interacts with postgres and base, think cursor()'''
engine = create_engine(CONNECTION, echo=True)

'''# 
class Student
    name varchar(40) PRIMARY KEY,
    instrument varchar(40) NOT NULL,
    lesson_time int,
    day_of_study varchar(40),
    age int,
    date_started datetime,
    keyword_comments varchar(100),
    status float
#'''
#################################
#################################
'''#
Student object dedicated to the creation of tables, inserting of rows, and manipulation of 
    SQL to all things pertaining to this program's STUDENTS(the target content!)...
    ...user=Teacher, target=Students...as time goes on, simply change the __tablename__

Below is a custom instance of Base, that uses all of it's sql interaction methods,
    but has params that are inherent to the table you will be interacting with
    within the desired db(db is defined in the CONNECTION variable)#'''
#class Student(Base):
#    #this is a class that inherits from the base instance of declarative_base()...
#    #...it is a holder of attributes that pertains to a TABLE...declarative base is a TABLE CREATOR!
#    #this method represents these params as *args, **kwargs!!
#    #db should be monthly and new table weekly!  __tablename__will be updated weekly!
#    #format will be students_monthdayyear
#    __tablename__= "students_08142022"
#    name = Column(String(25), primary_key=True)
#    instrument = Column(String(25), nullable=False)
#    contact_info = Column(String(40))
#    lesson_time = Column(Integer())
#    day_of_study = Column(String(25))
#    age = Column(Float())
#    date_entered = Column(DateTime(), default=datetime.utcnow)
#    keyword_comments = Column(String(50))
#    status = Column(Float())
    
#    def __repr__(self):
#        return f"<Student: {self.name}, {self.instrument}, {self.contact_info}, {self.lesson_time}, {self.day_of_study}, {self.age}, {self.date_entered}, {self.keyword_comments}, {self.status}>"

'''Journal IS USED FOR KEEPING NOTES :)'''
class Journal(Base):
    '''#to be updated weekly on Sunday!
       format is as such MMDDYY...
    ...insync with students db''' 
    __tablename__ = "august_21_22"
    entry = Column(String())
    entry_date = Column(DateTime(), default=datetime.utcnow, primary_key=True)

'''#instantiate sessionmaker'''
Session = sessionmaker(engine)

'''#######################################################'''
'''########################END###########################'''


#defining that the base directory is THIS file   
#BASE_DIR = os.path.dirname(os.path.realpath(__file__))
#this is the string with which we use sqlalchemy to connect to postgress with the basedir, and specified DB NAME!
#DO NOT use pg_connector.conn!!! THIS IS IN PLACE OF THAT!
#con_str = "postgresql:///" + os.path.join(BASE_DIR, 'user_students_master_2022.db')

#this will be in a seperate db connection    
#class User(base):
#    __tablename__ = "Users_master"
#new_student = StudentFall2022(name='Cal Moffe', instrument='piano', keyword_comments='driven, perceptive')

#object used for login and execute_sql
#db_connect = dbConnector(cd.hostname,
#                         cd.db_name,
#                         cd.username,
#                         cd.pwd,
#                         cd.port_id,
#                         )
##object used for postgres connection, and close connection(has execute sql in it)
#pg_connector = PostgresConnector(db_connect.host,
#                                 db_connect.dbname,
#                                 db_connect.user,
#                                 db_connect.password,
#                                 db_connect.port,
#                                 )
#
#cols_to_insert = [
#                    "id",	
#                    "first_name",	
#                    "last_name",	
#                    "date_of_birth",	
#                    "ethnicity",	
#                    "gender",	
#                    "status",
#                    "entry_academic_period",	
#                    "exclusion_type",
#                    "act_composite",
#                    "act_math", 
#                    "act_english", 
#                    "act_reading", 
#                    "sat_combined", 
#                    "sat_math",
#                    "sat_verbal", 
#                    "sat_reading", 
#                    "hs_gpa", 
#                    "hs_city", 
#                    "hs_state", 
#                    "hs_zip",
#                    "email", 
#                    "entry_age", 
#                    "ged", 
#                    "english_2nd_language",
#                    "first_generation"
#                 ]
#
#sql_cols = '''
#        id int PRIMARY KEY,	
#        first_name  varchar NOT NULL,	
#        last_name  varchar NOT NULL,	
#        date_of_birth date,	
#        ethnicity varchar,	
#        gender varchar,	
#        status	varchar,
#        entry_academic_period varchar,	
#        exclusion_type	varchar,
#        act_composite float,
#        act_math float, 
#        act_english float, 
#        act_reading float, 
#        sat_combined float, 
#        sat_math float,
#        sat_verbal float, 
#        sat_reading float, 
#        hs_gpa float, 
#        hs_city varchar, 
#        hs_state varchar, 
#        hs_zip int,
#        email varchar, 
#        entry_age float, 
#        ged bool, 
#        english_2nd_language bool,
#        first_generation bool
#        '''	
#
#def sql_to_df(table_to_convert):
#    pg_connector.cur.execute(f"select * from {table_to_convert}")
#    rows = pg_connector.cur.fetchall()
#    new_dataframe = pd.DataFrame(data=rows, columns=cols_to_insert)
#    return new_dataframe 
#
#def upload_to_db(table_for_csv, csv_to_convert):
#    new = create(table_to_create=table_for_csv, formatted_cols=sql_cols)
#    #upload og csv to db
#    SQL_STATEMENT = f"""
#                    COPY {new} FROM STDIN WITH
#                        CSV
#                        HEADER
#                        DELIMITER AS ','
#                    """ 
#                    #converted csv info back into csv
#    df = pd.read_csv(csv_to_convert)
#    df = df.to_csv(new, header=df.columns, index=False, encoding='utf-8')
#    #open csv save it as an object, and upload to db
#    #my_file = open(df)
#    #print('file opened in memory!')
#    #copy sqlstatement, item=my_file
#    pg_connector.cur.copy_expert(sql=SQL_STATEMENT, file=df)
#    print('file copied to db')