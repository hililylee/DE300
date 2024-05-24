To see the output of the initial and updated AUC ROC, please refer to ml_pyspark.ipynb 

The wrapped up python script can be found in modifiedfunc.py and the .sh file is titled run_ml_pipeline.sh 

After the modification of steps 1 and 2, I noticed that the new AUC ROC had slightly increased from 0.8828 and a selected maximum tree depth of 4 to an AUC ROC of 0.8912 and a selected maximum tree depth of 6. By adding new features, the model was likely able to capture more complex relationships in the data. For instance, adding the squared terms can help in capturing non-linear relationships between features and the target variable.
