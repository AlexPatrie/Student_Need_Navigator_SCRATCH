# Student_Need_Navigator_SCRATCH

Welcome to the Student Need Navigator! 

Need Navigator: A tool implementing the use of Pyspark, SqlAlchemy, PostgreSQL, Textblob, Mlib, Seaborn, and Tensorflow to create a pipleline for the storage of data and educational performance analysis of students. Within those student entries there are fields that act as features('instrument', age, lesson time, what you worked on). There are also fields that are both categorical and numerical in nature that prompt the teacher to input both observational keywords(driven, redirection, etc) and a floating point number known as "Status" which are based on that week's lesson. "keywords" are observational, one-word monikers that describe the students performance in that week's lesson. "status" is a score that is the teacher's answer to the question, "How did it go with that student this week?". There is then is a subsequent script in this pipleline that extracts features/labels from the week's data where the features include all demographical information AND the subjective entries of Keywords and Status. The label/target column within this dataframe is WHAT you actually worked on in terms of lesson material. Here we then preprocess(vectorizes text, etc.) and interpret this data for the use of creating a student "Prescription" in terms of their needs on a particular subject at a particular point during their course of study. The "prescription" that is generated is a prediction concieved by a custom-fitted 1D convolutional neural network. This prediction then is fed into a different local Postgres database for the use of "looking up" past "prescriptions".  This tool is intended to be used by teachers to track the needs of their students, but may have a variety of uses within the workplace. The total features which are "learned" by the model are both subjective and objective in nature and thus the "prescription" is more organic because it begs the NN to "walk in the shoes" of the teacher with regard to observing the student performance. I also wanted to provide an outlet for teachers to just "vent". To track my feelings along my coding journey, I created a "Journal" and "Pencil" object with SQL Alchemy that allows the user to create a journal (new postgres table if not exists) and record their thoughts. In version 2, there will be the implementation of a "chat bot" that interacts with the teacher and basically "asks them how they are doing". So, this application is a "Teacher's Friend" that is both an observation tool, an analytical tool/Smart lesson planner, AND a "therapist" of sorts. I concieved this idea based on the current climate that exists in the field of teacher by which so many teachers feel under-appreciated and downtrodden. 

PROGRAM FEATURES:

** ROSE - A Pyspark jdbc connection object that is used as a "cursor" to manage and manipulate data throughout all stages of the pipeline.

** Student Data Creation and Management plus Journal(SQL Alchemy/Postgres)

** Data Standardization/Preprocessing(Spark, Pandas, Mlib, Tensorflow/Pytorch Preprocessing)

** Data Analysis(Seaborn) and Prediction(Tensorflow)