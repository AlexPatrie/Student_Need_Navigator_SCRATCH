from main_scratch import Journal, Session, engine, Base 
import pandas as pd
import numpy as np
'''######################################################'''

"""this is the connection details for this script interacting with db"""
CONNECTION = 'postgresql://postgres:07141989@localhost:5432/my_coding_journey'

"""#this autocreates a table based on the params"""
#Base.metadata.create_all(engine)

"""#use sqlalchemy to connect to postgres and add student info"""
local_session = Session(bind=engine)


class Pencil:
    def add_new_entry(self):
        new_entry = input(f"\033[94m{'Tell me what you are thinking: '}\033[1m")
        journal = Journal(entry=new_entry)
        local_session.add(journal)
        local_session.commit()
        
pencil = Pencil()
pencil.add_new_entry()



"I have to decided to implement an architecture that defines functions as methods within a controlling class that acts as a 'cursor' that is used to e ... (172 characters truncated) ... cript, we call the job action as a simple function with critical args in which the spark class is called, along with their config and desired method."

