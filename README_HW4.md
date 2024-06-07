Set-up: 

In my S3 bucket called de300-lily-bucket, I have an Airflow folder for this Homework titled, "AirflowHW4/" 
In this folder, I uploaded my heart_disease(in).csv, requirements.txt, and config_hd.toml
I also included a dags folder which contains my Python script

In order to view my Airflow, my MWAA Environment is called, "DE300LilyAirflow"
Click on the Airflow UI Link to view the HW4 Dag

The screenshot in this folder shows my final successful run for Dag with the workflow diagram. I got my best model to be the logistic regression model using standard EDA. 
Here were the results of the best model: 
[2024-06-07, 17:47:47 UTC] {{logging_mixin.py:188}} INFO - Accuracy of the best model on the testing data: 0.7738095238095238
[2024-06-07, 17:47:47 UTC] {{logging_mixin.py:188}} INFO - Precision of the best model on the testing data: 0.7572815533980582
[2024-06-07, 17:47:47 UTC] {{logging_mixin.py:188}} INFO - Recall of the best model on the testing data: 0.8571428571428571
[2024-06-07, 17:47:47 UTC] {{logging_mixin.py:188}} INFO - F1 Score of the best model on the testing data: 0.8041237113402062
