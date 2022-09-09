'''THIS SERVES AS THE PRIMARY ACTIVATION FUNCTION'''

def execute():
    from data_intake_job1 import main_data_intake
    from data_intake_job1 import project_dir as di_dir 
    from preprocessing_job2 import main_preprocessing
    from preprocessing_job2 import project_dir as pp_dir
    main_data_intake(di_dir)
    main_preprocessing(pp_dir)
    
if __name__ == "__main__":
    execute()  

