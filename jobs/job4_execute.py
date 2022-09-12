import typing 

'''THIS SERVES AS THE PRIMARY ACTIVATION FUNCTION'''
def execute(journal:bool=False, create_students:bool=False, process_data:bool=False):
    from job0_journal import main_journal
    from job1_create_students import main_create_students
    from job2_data_intake import main_data_intake
    from job2_data_intake import project_dir as di_dir 
    from job3_preprocessing import main_preprocessing
    from job3_preprocessing import project_dir as pp_dir
    main_create_students()
    main_data_intake(di_dir)
    #main_preprocessing(pp_dir)
    
if __name__ == "__main__":
    execute()  

