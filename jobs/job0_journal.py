import logging
import os 
from time import asctime
import sys 


#PRIMARY ACTIVATION EXECUTION
def main_journal():
    """LOGGING BLOCK"""
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
    LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
    logger = logging.getLogger('log4j2')
    sys.path.insert(1, project_dir)

    """just a note that Pencil's grandparent is BASE, and thus 
       journal is never explicitly called. You cannot write in your
       real-life journal without a Pencil() right??? :)""" 
    from sqlAlch_class import Pencil       
    
    """EXE BLOCK"""
    pencil = Pencil()
    return pencil.add_new_entry()


if __name__ == "__main__":
   main_journal()





