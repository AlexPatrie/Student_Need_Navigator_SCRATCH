import typing 

'''THIS SERVES AS THE PRIMARY ACTIVATION FUNCTION'''
def execute(new_month:bool=False):
    from job0_journal import main_journal
    from job1_create_students import main_create_students
    from job2_data_intake import main_data_intake
    from job2_data_intake import project_dir as di_dir 
    from job3_preprocessing import main_preprocessing
    from job3_preprocessing import project_dir as pp_dir
    
    def execute_jobs(journal:bool=False, create_students:bool=False):
        if journal:
            main_journal()
        if create_students:
            main_create_students()
        #main_data_intake(di_dir)
        main_preprocessing(pp_dir)
        
    return execute_jobs(journal=True, create_students=True) if new_month \
           else execute_jobs()
    
if __name__ == "__main__":
    execute()  

