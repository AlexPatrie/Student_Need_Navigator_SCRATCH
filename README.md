# Student_Need_Navigator_SCRATCH

Welcome to the Student Need Navigator! 

Need Navigator: A tool implementing the use of Pyspark, SqlAlchemy, PostgreSQL, Textblob, Mlib, Seaborn, and Tensorflow to create a pipleline for the storage of data and educational performance analysis of students. Within those student entries there are fields that act as features('instrument', age, lesson time, what you worked on). There are also fields that are both categorical and numerical in nature that prompt the teacher to input both observational keywords(driven, redirection, etc) and a floating point number known as "Status" which are based on that week's lesson. "keywords" are observational, one-word monikers that describe the students performance in that week's lesson. "status" is a score that is the teacher's answer to the question, "How did it go with that student this week?". There is then is a subsequent script in this pipleline that extracts features/labels from the week's data where the features include all demographical information AND the subjective entries of Keywords and Status. The label/target column within this dataframe is WHAT you actually worked on in terms of lesson material. Here we then preprocess(vectorizes text, etc.) and interpret this data for the use of creating a student "Prescription" in terms of their needs on a particular subject at a particular point during their course of study. The "prescription" that is generated is a prediction concieved by a custom-fitted 1D convolutional neural network. This prediction then is fed into a different local Postgres database for the use of "looking up" past "prescriptions".  This tool is intended to be used by teachers to track the needs of their students, but may have a variety of uses within the workplace. The total features which are "learned" by the model are both subjective and objective in nature and thus the "prescription" is more organic because it begs the NN to "walk in the shoes" of the teacher with regard to observing the student performance. I also wanted to provide an outlet for teachers to just "vent". To track my feelings along my coding journey, I created a "Journal" and "Pencil" object with SQL Alchemy that allows the user to create a journal (new postgres table if not exists) and record their thoughts. In version 2, there will be the implementation of a "chat bot" that interacts with the teacher and basically "asks them how they are doing". So, this application is a "Teacher's Friend" that is both an observation tool, an analytical tool/Smart lesson planner, AND a "therapist" of sorts. I concieved this idea based on the current climate that exists in the field of teacher by which so many teachers feel under-appreciated and downtrodden. 

PROGRAM FEATURES:

** ROSE - A Pyspark jdbc connection object that is used as a "cursor" to manage and manipulate data throughout all stages of the pipeline.

** Student Data Creation and Management plus Journal(SQL Alchemy/Postgres)

** Data Standardization/Preprocessing(Spark, Pandas, Mlib, Tensorflow/Pytorch Preprocessing)

** Data Analysis(Seaborn) and Prediction(Tensorflow)


Here is the proposed process for the pipeline:

++Student Entry(SQL Alchemy/Postgress) =>
    -Here the user inputs student demographical data as well as lesson observations 
     and content of the last lesson. Cols are as such:
        -name
        -instrument
        -age
        -contact info
        -day of study
        -time of study
        -keyword observations of the last lesson
        -Status(from 1 to 5 ascending, how is it going with the student based off of this week?)
        -Material Covered
    -The table gets put into a Postgres db under a db that is unique for each month
     in the year, where each month is a db.
    
++Read and Clean/Transform(SQL->Pandas->Spark) =>
    -Here the table(entries for the week) is read out of Postgres and into 
     a dataframe with pandas.read_sql
    -The df is then converted into a spark df so that it can be extensively manipulated.
    -A spark connector class is created that has methods which make a spark connection,
     read csvs or tables, drop/fill na, and more.
    -Schema dtypes are regularized such that all str are str, and all nums are float
    -A key part to that spark class is the ability to visualize the information. 
     it gets inherited by another class in the next script! 

++Analyze and Visualize the Data(Spark/Pandas->Seaborn/TextBlob) =>
    -Here we determine correlations based on descriptive elements of
     the df...so using elements that are custom determined and also from 
     pandasProfilingReport to create visualizations for correlation. This 
     will help us with feature extraction.

++Preprocess The Dataframe(Spark/Pandas->NLTK->Tensorflow) =>
    -Here the dataframe is preprocessed. 
    -Numerical Features are regularized(divided by the max() value of the group)
    -Categorical features are vectorized(using lemmatization, and tokenization) and
     also regularized
    -X and y are defined, however train, test, validation are NOT

++Create model and make predictions based on the preprocessed data(Tensorflow->Spark/Pandas->SqlAlch->Postgres) =>
    -Partition the preprocessed data such that 80%(train) 10%(Validation)
     and 10%(Test)
    -Establish Embedding and Vectorization layers that require a specific data shape
    -Reshape data and fit the embedding layer with the preprocessed data
    -Create the architecture of a 1D CNN that takes the preprocessed data as
     input of specific shape
    -Output predictions based on the labels of the data, in this case what we want 
     to predict(what the student should be studying)
    -Use Spark to create a df out of the predictions
    -SparkDf to SqlAlch create table into a predictions db within Postgres

++Interact with predictions(Spark) =>
    -Loop back the predictions by appending the predictions to the original student
     name as foregin key
    -Provide a final 'historical' df with all data, observations, and predictions.
    -Traverse the history based on user preferences.
