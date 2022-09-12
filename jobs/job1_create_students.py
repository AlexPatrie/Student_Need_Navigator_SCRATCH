import typing 
from typing import Optional
from unicodedata import name
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine 
import os
import logging 
import sys 



'''this ones tricky bc there are alot of contrasts between private and public
variables and methods...silly me'''

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



CONNECTION = 'postgresql://postgres:07141989@localhost:5432/students_sept_2022'
BASE = declarative_base()
ENGINE = create_engine(CONNECTION, echo=True)
SESSION = sessionmaker(ENGINE)
local_session = SESSION(bind=ENGINE)

from sqlAlch_class import Student, Navigator 

#HOUSING JOB ACTIONS
def main_create_students(): 
    import student_data as sdata #<-the SOURCE of the student data in py form
    insert_students(sdata.sept_0422_1) #<-BATCH1 to be replaced with txt file!
    insert_students(sdata.sept_0422_2) #<-BATCH2
    
    #this func gives you the ability to input a student if you only have data of shape n
    insert_ragged_student(name='Sorbeta', loc='NHmc', material='followed book to next songs', how_it_went_score=1.5)
    insert_ragged_student(name='Zack', material='Discussing the target solution for his capstone project.')
    insert_ragged_student(name='Rose', loc='Home', material='Discussing the future of caretaking', how_it_went_score=5)
    #prompt_insert_student()
    
#JOB SPECIFIC ACTIONS THAT ACTIVATE JOB-SPECIFIC CLASS METHODS
def insert_ragged_student(name:Optional[str]="NONE", instrument:Optional[str]="NONE", 
                          loc:Optional[str]="NONE", material:Optional[str]="NONE", 
                          how_it_went_score:Optional[float]=0):
    """USE THIS FUNCTION IF YOU DO NOT HAVE A VALUE FOR EVERY FEATURE(ragged data)
       ARGS ARE TAKEN FROM THE class Student"""
    new_student = Student(name=name, instrument=instrument, lesson_location=loc, 
                          lesson_material=material, status=how_it_went_score)
    local_session.add(new_student)
    local_session.commit()
    
def prompt_insert_student():
    """PROMPTS USER TO ADD FEATURES INDIVIDUALLY.
       **TO BE REPLACED WITH pyttsx3/Tensorflow**"""
    return Navigator().prompt_batch_insert()
      
def insert_students(new_batch):   
    """new batch to be inserted"""                   
    return Navigator().insert_batch(batch_info=new_batch)


#PRIMARY ACTIVATION FUNCTION (removed completely in job2 to prefer critical activation function) 
#if __name__ == '__main__':
#    main_create_students()                
