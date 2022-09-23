
'''THIS SERVES AS THE PRIMARY ACTIVATION FUNCTION'''
def execute_jobs(new_month:bool=False):
    from job1_create_students import main_create_students
    from job2_data_intake import main_data_intake
    from job2_data_intake import project_dir as di_dir 
    from job3_preprocessing import main_preprocessing
    from job3_preprocessing import project_dir as pp_dir
    
    def execute_jobs(create_students:bool=False):
        if create_students:
            main_create_students()
        #main_data_intake(di_dir)
        main_preprocessing(pp_dir)
    
    return execute_jobs(create_students=True) if new_month \
           else execute_jobs()
           
def open_journal(journal:bool=True):
    from job0_journal import main_journal
    if journal:
        return main_journal()


#change these to control job actions!   
JOBS = True            
JOURNAL = False 
if __name__ == "__main__":
    if JOBS:
        execute_jobs()#<-if you want to create new students, arg new_month=True
    if JOURNAL:
        open_journal() #<-no arg to NOT use journal

