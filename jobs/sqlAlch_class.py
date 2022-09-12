import typing
from typing import Optional
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, DateTime, Integer, Float, create_engine 
from datetime import datetime
from connectionDetails import THIS_WEEK
import connectionDetails as cd 



'''method used to create an f string containing the sqlalchemy conn string
    required to connect to postgress via sqlalchemy.create_engine()'''
def define_connection(dialect_and_driver, username, password, host, port, db_name:Optional[str]=cd.september_22_db):
    connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
    return connection_string

'''instance of a correctly formmated sqlalchemy conn string using admin logindetails'''
CONNECTION = define_connection(cd.conn_type, cd.username, cd.pwd, 
                               cd.hostname, cd.port_id, cd.september_22_db)

'''Alternatively, these are the connection details for this script interacting 
with journal db in preformatted str form'''
CONNECTION_STR = 'postgresql://postgres:07141989@localhost:5432/my_coding_journey'

'''that which pertains to a database in sqla'''
BASE = declarative_base()

'''that which interacts with postgres and base, think cursor()'''
ENGINE = create_engine(CONNECTION, echo=True)

'''instantiate sessionmaker'''
SESSION = sessionmaker(ENGINE)



###########################################################################################
'''Student object dedicated to the creation of tables, inserting of rows, and manipulation of 
    SQL to all things pertaining to this program's STUDENTS(the target content!)...
    ...user=Teacher, target=Students...as time goes on, simply change the __tablename__

Below is a custom instance of Base, that uses all of it's sql interaction methods,
    but has params that are inherent to the table you will be interacting with
    within the desired db(db is defined in the CONNECTION variable)
    
PLEASE NOTE THAT A JOURNAL WILL BE CREATED IN THE SAME DB AS STUDENTS. THAT IS OKAY
AND AS A MATTER OF FACT WHAT WE WANT IN OUR FUNCTIONALITY!'''

class Student(BASE):
    '''this is a class that inherits from the base instance of declarative_base()...
    ...it is a holder of attributes that pertains to a TABLE...declarative base is a TABLE CREATOR!
    this method represents these params as *args, **kwargs!!
    db should be monthly and new table weekly!  
     __tablename__will be updated weekly!
    format will be students_monthdayyear'''
    __tablename__= f"students_{THIS_WEEK}"
    #perhaps we should create a student_id that is primary/for key?
    name = Column(String(25), primary_key=True)
    instrument = Column(String(25))
    lesson_location = Column(String(10))
    contact_info = Column(String(40))
    lesson_time = Column(Integer())
    day_of_study = Column(String(25))
    age = Column(Float())
    lesson_material = Column(String(200), nullable=False) #<-here nullable false because there will always be AT LEAST SOMETHING to record from our meeting even if not a lesson
    keyword_comments = Column(String(50))
    status = Column(Float())
    
#######################################################
    

#Journal IS USED FOR KEEPING NOTES :)
class Journal(BASE):
    '''to be updated weekly on Sunday!
       format is as such MMDDYY...
    ...insync with students db''' 
    __tablename__ = f"journal_{THIS_WEEK}"
    entry = Column(String())
    entry_date = Column(DateTime(), default=datetime.utcnow, primary_key=True)

########################################################

class Pencil():
    '''use sqlalchemy to connect to postgres and add student info'''
    def __init__(self):
        '''this autocreates a table based on the params'''
        self.local_session = SESSION(bind=ENGINE)
        BASE.metadata.create_all(ENGINE)

    def add_new_entry(self):
        #BASE.metadata.create_all(ENGINE)
        new_entry = input(f"\033[94m{'Tell me what you are thinking: '}\033[1m")
        journal = Journal(entry=new_entry)
        self.local_session.add(journal)
        self.local_session.commit()
 
########################################################

###############################CLASS START########################################## 
'''Navigator class for extra functionality for batch inserts!'''
class Navigator():
    
    def __init__(self):
        '''this autocreates a table based on the params'''
        self.local_session = SESSION(bind=ENGINE)
        BASE.metadata.create_all(ENGINE)
        
    #method for batch inserting that iterates over batch list 
    def insert_batch(self, batch_info):
        row = 0
        while row < len(batch_info):
            new_student = Student(name=batch_info[row][0], instrument=batch_info[row][1], 
                                  lesson_location=batch_info[row][2], contact_info=batch_info[row][3], 
                                  lesson_time=batch_info[row][4], day_of_study=batch_info[row][5], 
                                  age=batch_info[row][6], lesson_material=batch_info[row][7], 
                                  keyword_comments=batch_info[row][8], status=batch_info[row][9])
            self.local_session.add(new_student)
            self.local_session.commit()
            #visually printing confirmation for my own sanity!
            print('\n***new student added!!!***\n')
            row += 1
                                      
#########################################################################  
    
    def prompt_batch_insert(self):         
        '''method that prompts user to add info to batch_list'''   
        batch = []
        amt_to_enter = int(input("How many students would you like to add?: "))
        if amt_to_enter > 0:
            for amt in range(amt_to_enter):
                name = input("Student's name: ")
                inst = input("Instrument: ")
                loc = input("Lesson Location: ")
                contact = input("Method of contact: ")
                time = int(input("Time of lesson: "))
                day = input("Lesson day: ")
                age = int(input("Age: "))
                lesson_material = input("What did you work on with this student? ")
                keywords = input("Any keywords to describe student: ")
                status = int(input("How well is it going with this student?(1lowest-5highest): "))
                new_student_info = (name, inst, loc, contact, time, day, age, lesson_material, keywords, status)
                batch.append(new_student_info)
        return self.insert_batch(batch)
            