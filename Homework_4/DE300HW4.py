from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import StandardScaler
from scipy import stats



# Define the default args dictionary for DAG
default_args = {
    'owner': 'lilylee',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

def reads3() -> dict:
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket="de300-lily-bucket", Key="AirflowHW4/config_hd.toml")
    config_data = obj['Body'].read().decode('utf-8')
    params = tomli.loads(config_data)
    return params


PARAMS = reads3()


def load_data():

    # Connect to S3
    s3 = boto3.client('s3')

    # Specify the bucket and file key
    bucket_name = "de300-lily-bucket"
    file_key = "AirflowHW4/heart_disease(in).csv"

    # Download the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    # Read the CSV content into a DataFrame
    df = pd.read_csv(StringIO(csv_content))

    return df


def clean_impute_sklearn(**kwargs):

    # Retrieve the DataFrame from the context
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='load_data') 


    # Drop unnecessary rows at the end of wrongly formatted data
    df = df.head(899)

    # Change the column name ekgday(day
    df.rename(columns={"ekgday(day": "ekgday"}, inplace=True)

    # Fill missing categorical values with the mode
    categorical_columns = ["painloc", "painexer", "relrest", "cp", "sex", "htn", "smoke",
                        "fbs", "dm", "famhist", "restecg", "dig", "prop", "nitr",
                        "pro", "diuretic", "proto", "exang", "xhypo", "slope", "ca",
                        "exerckm", "restwm", "exerwm", "thal", "thalsev", "thalpul", "earlobe"]
    for col in categorical_columns:
        mode_value = df[col].mode()[0]
        df[col].fillna(mode_value, inplace=True)

    # Update pncaden column with the sum of painloc, painexer, and relrest
    df['pncaden'] = df[['painloc', 'painexer', 'relrest']].sum(axis=1)

    # Replace missing values in restckm column with zeroes
    df['restckm'].fillna(0, inplace=True)

    # Impute missing numeric values with mean
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    imputer = SimpleImputer(strategy='mean')
    df[numeric_columns] = imputer.fit_transform(df[numeric_columns])


    # Cleaning process of removing outliers using Z-score method
    z_scores = stats.zscore(df[numeric_columns], nan_policy='omit')

    # Set a threshold for Z-score
    threshold = 6

    # Create a boolean mask where True indicates the row is an outlier
    outliers = (np.abs(z_scores) > threshold).any(axis=1)

    # Filter the DataFrame to remove outliers
    df_sklearn_clean = (df[~outliers])


    return df_sklearn_clean

def feature_eng_1(**kwargs):

    # Retrieve the DataFrame from the context
    ti = kwargs['ti']
    df_sklearn_clean = ti.xcom_pull(task_ids='clean_impute_sklearn')  

    # Feature transformation - Log transform
    columns_to_log_transform = ["cigs", "years", "rldv5e"]
    log_transformer = FunctionTransformer(np.log1p)
    df_sklearn_clean[columns_to_log_transform] = log_transformer.transform(df_sklearn_clean[columns_to_log_transform])

    # Feature transformation - Round to 2 digits
    numeric_columns = df_sklearn_clean.select_dtypes(include=[np.number]).columns.tolist()
    df_sklearn_clean[numeric_columns] = df_sklearn_clean[numeric_columns].round(2)

    df_fe1 = df_sklearn_clean

    return df_fe1


def clean_impute_spark(**kwargs):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, lit, avg
    from pyspark.sql.types import StructType, StructField, StringType

    # Retrieve the DataFrame from the context
    ti = kwargs['ti']
    df_pandas = ti.xcom_pull(task_ids='load_data') 

    spark = SparkSession.builder.appName("HeartDiseaseEDA").getOrCreate()

    # Convert the Pandas DataFrame to a Spark DataFrame
    schema = StructType([StructField(col_name, StringType(), True) for col_name in df_pandas.columns])
    df = spark.createDataFrame(df_pandas, schema=schema)

    # Limit the DataFrame to the first 899 rows
    df = df.limit(899)

    # Change the column name ekgday(day
    df = df.withColumnRenamed("ekgday(day", "ekgday")

    # Function to fill missing values with mode
    def fill_mode(df, column):
        mode_row = df.filter(col(column).isNotNull()) \
                     .groupBy(column).count() \
                     .orderBy('count', ascending=False) \
                     .first()
        mode = mode_row[0] if mode_row else None
        return df.withColumn(column, when(col(column).isNull(), lit(mode)).otherwise(col(column)))

    # Replace missing values for painloc and painexer with the mode of the columns
    df = fill_mode(df, 'painloc')
    df = fill_mode(df, 'painexer')

    # Replace values less than 100 mm Hg for trestbps with the average of the column
    mean_trestbps = df.filter(col('trestbps').isNotNull()).select(avg(col('trestbps'))).first()[0]
    df = df.withColumn('trestbps', when((col('trestbps') < 100) | col('trestbps').isNull(), lit(mean_trestbps)).otherwise(col('trestbps')))

    # Replace values less than 0 and greater than 4 for oldpeak with the average of the column
    mean_oldpeak = df.filter(col('oldpeak').isNotNull()).select(avg(col('oldpeak'))).first()[0]
    df = df.withColumn('oldpeak', when((col('oldpeak') < 0) | (col('oldpeak') > 4) | col('oldpeak').isNull(), lit(mean_oldpeak)).otherwise(col('oldpeak')))

    # Replace missing values for thaldur and thalach with the average of the columns
    mean_thaldur = df.filter(col('thaldur').isNotNull()).select(avg(col('thaldur'))).first()[0]
    mean_thalach = df.filter(col('thalach').isNotNull()).select(avg(col('thalach'))).first()[0]
    df = df.withColumn('thaldur', when(col('thaldur').isNull(), lit(mean_thaldur)).otherwise(col('thaldur')))
    df = df.withColumn('thalach', when(col('thalach').isNull(), lit(mean_thalach)).otherwise(col('thalach')))

    # Replace missing values and values greater than 1 for fbs, prop, nitr, pro, and diuretic with the mode
    columns_to_fill_mode = ['fbs', 'prop', 'nitr', 'pro', 'diuretic']
    for column in columns_to_fill_mode:
        mode = df.filter((col(column).isNotNull()) & (col(column) <= 1)).groupBy(column).count().orderBy('count', ascending=False).first()[0]
        df = df.withColumn(column, when((col(column).isNull()) | (col(column) > 1), lit(mode)).otherwise(col(column)))

    # Replace missing values for exang and slope with the mode of the columns
    df = fill_mode(df, 'exang')
    df = fill_mode(df, 'slope')

    # Drop smoke column
    df = df.drop('smoke')

    # Keep certain columns only
    columns_to_keep = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'fbs',
                       'prop', 'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang',
                       'oldpeak', 'slope', 'target']

    
    df_spark_clean= df.select(*columns_to_keep)

    # Convert the cleaned Spark DataFrame to a Pandas DataFrame for XCom serialization
    df_spark_clean = df_spark_clean.toPandas()

    # Stop the Spark session
    spark.stop()

    # Push the cleaned DataFrame as a Pandas DataFrame to XCom
    ti.xcom_push(key='df_clean', value=df_spark_clean)

    return df_spark_clean



def feature_eng_2(**kwargs):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    import pandas as pd

    # Retrieve the DataFrame from the context
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='clean_impute_spark', key='df_clean')


    # Convert to Spark DataFrame
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()

    # Convert the Pandas DataFrame to a Spark DataFrame
    schema = StructType([StructField(col_name, StringType(), True) for col_name in df_dict.columns])
    df_spark_clean = spark.createDataFrame(df_dict, schema=schema)


    # Cast string columns to numeric types
    for col_name in df_spark_clean.columns:
        df_spark_clean = df_spark_clean.withColumn(col_name, col(col_name).cast(DoubleType()))

    # Select the desired columns
    feature_columns = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'fbs',
                       'prop', 'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang',
                       'oldpeak', 'slope','target']

    # Filter the DataFrame to retain only the selected feature columns
    df_spark_features = df_spark_clean.select(*feature_columns)

    # Remove rows with NaN values
    df_spark_features = df_spark_features.na.drop()

    # Assemble features into a feature vector
    assembler = VectorAssembler(inputCols=feature_columns[:-1], outputCol="features")
    df_spark_features_assembled = assembler.transform(df_spark_features)

    # Standardize features
    scaler = StandardScaler(inputCol='features', outputCol='scaled_features', withStd=True, withMean=True)
    scaler_model = scaler.fit(df_spark_features_assembled)
    df_fe2 = scaler_model.transform(df_spark_features_assembled)

    # Convert the PySpark DataFrame to a Pandas DataFrame
    df_fe2 = df_spark_features.toPandas()

    # Stop the Spark session
    spark.stop()

    # Push the cleaned DataFrame as a Pandas DataFrame to XCom
    ti.xcom_push(key='df_fe2', value=df_fe2)

    return df_fe2


def web_scrape():

    import re
    import urllib.request

    # Extract content from source 1 (Australia data)
    url1 = 'https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking-and-vaping/latest-release'
    response1 = urllib.request.urlopen(url1)
    html1 = response1.read().decode('utf-8')

    # Use regex to find the second table in the HTML
    table_pattern = re.compile(r'<table class="responsive-enabled".*?>(.*?)</table>', re.DOTALL)
    tables = table_pattern.findall(html1)
    if len(tables) >= 2:  # Ensure at least two tables are found
        table1_html = tables[1]

    # Initialize list to store smoking rates
    smoking_rates = []

    # Regex patterns for extracting data
    row_pattern = re.compile(r'<tr.*?>(.*?)</tr>', re.DOTALL)
    col_pattern = re.compile(r'<td.*?>(.*?)</td>', re.DOTALL)
    age_pattern = re.compile(r'<th scope="row" class="row-header">(.*?)</th>', re.DOTALL)

    # Extract data from each row
    for i in range(1, 8):
        # Extract row HTML
        row_html = row_pattern.findall(table1_html)[i]

        # Extract smoking rate from columns
        cols = col_pattern.findall(row_html)
        if len(cols) >= 10:  # Ensure at least 10 columns exist
            smoke_rate = float(cols[9])
        else:
            print(f"Error: Insufficient columns in row {i}")
            continue

        # Extract age range from row header
        age_range = age_pattern.findall(row_html)[0]
        age_values = age_range.replace('–', '-').split('-')
        age_start = int(age_values[0])
        age_end = int(age_values[1])

        # Create dictionary mapping ages to smoking rate
        age_to_smoke_rate = {age: smoke_rate for age in range(age_start, age_end + 1)}

        # Append to smoking rates list
        smoking_rates.append(age_to_smoke_rate)


    # Add senior age group data
    senior_cols = row_pattern.findall(table1_html)[8]
    pattern = r'<th scope="row" class="row-header">(\d+) years and over</th>'
    age_match = re.search(pattern, senior_cols)
    senior_age = age_match.group(1)

    # Find senior smoke rate
    pattern = r'<td class="data-value">([\d.]+)</td>'
    matches = re.findall(pattern, senior_cols)
    senior_smoke_rate = matches[9]

    print(senior_age)
    print(senior_smoke_rate)

    # Extract content from source 2 (US data)
    url2 = 'https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm'
    response2 = urllib.request.urlopen(url2)
    html2 = response2.read().decode('utf-8')

    # Use regex to find the block list in the HTML
    block_list_pattern = re.compile(r'<ul class="block-list">(.*?)</ul>', re.DOTALL)
    block_lists = block_list_pattern.findall(html2)
    desired_lines = re.findall(r'<li>(.*?)</li>', block_lists[1], re.DOTALL)[:4]

    def extract_numbers(text):
        # Extracting numbers based on specific patterns
        numbers = []
        for sentence in text:
            # Split the sentence into words
            words = sentence.split()
            for word in words:
                if word.isdigit():  # Check if the word is a digit
                    numbers.append(word)
                elif '&ndash;' in word:   # Check for en dash representing age range
                    ages = word.split('&ndash;')
                    numbers.extend(ages)
                elif '%' in word:   # Check for percentage
                    numbers.append(word.strip('()%'))
        return numbers


    # Use regex to find the block list in the HTML
    block_list_pattern = re.compile(r'<ul class="block-list">(.*?)</ul>', re.DOTALL)
    block_lists = block_list_pattern.findall(html2)
    desired_lines = re.findall(r'<li>(.*?)</li>', block_lists[1], re.DOTALL)[:4]

    # Define the regex pattern to split the lines
    pattern = r'\b(?:About|Nearly)\s*(\d+)\s*of every\s*(\d+)\s*adults aged (\d+)&ndash;(\d+) years\s*\((\d+\.\d+)%\)'

    # Split each line into four sections using regex and assign them to separate variables
    desired_line1 = re.split(pattern, desired_lines[0])
    desired_line2 = re.split(pattern, desired_lines[1])
    desired_line3 = re.split(pattern, desired_lines[2])
    desired_line4 = re.split(pattern, desired_lines[3])

    # Remove empty strings from the list of sections
    group1_data = [section for section in desired_line1 if section]
    group2_data = [section for section in desired_line2 if section]
    group3_data = [section for section in desired_line3 if section]
    group4_data = extract_numbers([section for section in desired_line4 if section])

    return smoking_rates, senior_age, senior_smoke_rate, group1_data, group2_data, group3_data, group4_data, html2


def merge(**kwargs):

    from pyspark.sql.functions import when, col, lit
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    import pandas as pd
    import re
    import urllib.request

    ti = kwargs['ti']
    
    df_fe2 = ti.xcom_pull(task_ids='feature_eng_2')
    smoking_rates, senior_age, senior_smoke_rate, group1_data, group2_data, group3_data, group4_data, html2 = web_scrape()

    # Convert to Spark DataFrame
    spark = SparkSession.builder.appName("Merge").getOrCreate()

    # Convert the Pandas DataFrame to a Spark DataFrame
    schema = StructType([StructField(col_name, StringType(), True) for col_name in df_fe2.columns])
    df_fe2 = spark.createDataFrame(df_fe2, schema=schema)


    #Update DataFrame with smoking rates from source 1
    df_fe2 = df_fe2.withColumn('smokesource1', lit(None).cast('double'))
    df_fe2 = df_fe2.withColumn('smokesource2', lit(None).cast('double'))

    for age_to_smoke_rate in smoking_rates:
        for age, smoking_rate in age_to_smoke_rate.items():
            df_fe2 = df_fe2.withColumn('smokesource1', when(col('age') == age, lit(smoking_rate)).otherwise(col('smokesource1')))
    if 'senior_age' in locals() and 'senior_smoke_rate' in locals():
        df_fe2 = df_fe2.withColumn('smokesource1', when(col('age') >= senior_age, lit(senior_smoke_rate)).otherwise(col('smokesource1')))

    # Function to update DataFrame with smoking rates based on age groups
    def update_smoking_rates(df_fe2, age_start, age_end, smoking_rate):
        return df_fe2.withColumn('smokesource2',
                            when((col('sex') == 0) & (col('age') >= age_start) & (col('age') <= age_end), smoking_rate)
                            .otherwise(col('smokesource2')))

    # Age group 1
    age_start1, age_end1 = int(group1_data[2]), int(group1_data[3])
    smoking_rate1 = float(group1_data[4])
    df_fe2 = update_smoking_rates(df_fe2, age_start1, age_end1, smoking_rate1)

    # Age group 2
    age_start2, age_end2 = int(group2_data[2]), int(group2_data[3])
    smoking_rate2 = float(group2_data[4])
    df_fe2 = update_smoking_rates(df_fe2, age_start2, age_end2, smoking_rate2)

    # Age group 3
    age_start3, age_end3 = int(group3_data[2]), int(group3_data[3])
    smoking_rate3 = float(group3_data[4])
    df_fe2 = update_smoking_rates(df_fe2, age_start3, age_end3, smoking_rate3)

    # Age group 4
    age_start4 = int(group4_data[2])
    smoking_rate4 = float(group4_data[3])
    df_fe2 = df_fe2.withColumn('smokesource2',
                    when((col('sex') == 0) & (col('age') >= age_start4), smoking_rate4)
                    .otherwise(col('smokesource2')))

    # Extract relevant lines containing smoke rates for men and women
    block_list_gender_text = re.findall(r'<ul class="block-list">(.*?)</ul>', html2, re.DOTALL)[0]

    # Use regular expressions to extract lines containing smoke rates for men and women
    men_smoke_rate = float(re.findall(r'<li class="main">.*men \((.*?)%\)</li>', block_list_gender_text)[0])
    women_smoke_rate = float(re.findall(r'<li class="main">.*women \((.*?)%\)</li>', block_list_gender_text)[0])

    gender_ratio = men_smoke_rate / women_smoke_rate

    men_smoke1 = (float(group1_data[4])) * gender_ratio
    men_smoke2 = (float(group2_data[4])) * gender_ratio
    men_smoke3 = (float(group3_data[4])) * gender_ratio
    men_smoke4 = (float(group4_data[3])) * gender_ratio

    # Function to update DataFrame with smoking rates for males based on age groups
    def update_smoking_rates_males(df_fe2, age_start, age_end, smoking_rate):
        return df_fe2.withColumn('smokesource2',
                            when((col('sex') == 1) & (col('age') >= age_start) & (col('age') <= age_end), smoking_rate)
                            .otherwise(col('smokesource2')))

    # Age group 1
    age_start1, age_end1 = int(group1_data[2]), int(group1_data[3])
    df_fe2 = update_smoking_rates_males(df_fe2, age_start1, age_end1, men_smoke1)

    # Age group 2
    age_start2, age_end2 = int(group2_data[2]), int(group2_data[3])
    df_fe2 = update_smoking_rates_males(df_fe2, age_start2, age_end2, men_smoke2)

    # Age group 3
    age_start3, age_end3 = int(group3_data[2]), int(group3_data[3])
    df_fe2 = update_smoking_rates_males(df_fe2, age_start3, age_end3, men_smoke3)

    # Age group 4
    age_start4 = int(group4_data[2])
    df_fe2 = df_fe2.withColumn('smokesource2',
                    when((col('sex') == 1) & (col('age') >= age_start4), men_smoke4)
                    .otherwise(col('smokesource2')))

    # Calculate average of smokesource1 and smokesource2 columns and replace smoke column
    df_fe2 = df_fe2.withColumn('smoke', (col('smokesource1') + col('smokesource2')) / 2)

    # Drop smokesource1 and smokesource2 columns
    df_fe2 = df_fe2.drop('smokesource1', 'smokesource2') ################
    

    # Convert the cleaned Spark DataFrame to a Pandas DataFrame for XCom serialization
    df_merge = df_fe2.toPandas()

    # Stop the Spark session
    spark.stop()

    # Push the cleaned DataFrame as a Pandas DataFrame to XCom
    ti.xcom_push(key='df_scrape_merge', value=df_merge)

    return df_merge


def lr_model_df1(**kwargs):

    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score

    ti = kwargs['ti']

    df_fe1 = ti.xcom_pull(task_ids='feature_eng_1')
    X = df_fe1.drop(columns=['target'])
    y = df_fe1['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    lr = LogisticRegression()
    lr.fit(X_train, y_train)
    lr_predictions = lr.predict(X_test)
    lr1_accuracy = accuracy_score(y_test, lr_predictions)

    return lr1_accuracy



def lr_model_df2(**kwargs):

    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score

    ti = kwargs['ti']

    df_fe2 = ti.xcom_pull(task_ids='feature_eng_2')



    X = df_fe2.drop(columns=['target'])
    y = df_fe2['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    lr = LogisticRegression()
    lr.fit(X_train, y_train)
    lr_predictions = lr.predict(X_test)
    lr2_accuracy = accuracy_score(y_test, lr_predictions)

    return lr2_accuracy




def lr_model_dfmerge(**kwargs):

    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score
    ti = kwargs['ti']
    df_merge = ti.xcom_pull(task_ids='merge')


    X = df_merge.drop(columns=['target'])
    y = df_merge['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    lr = LogisticRegression()
    lr.fit(X_train, y_train)
    lr_predictions = lr.predict(X_test)
    lrm_accuracy = accuracy_score(y_test, lr_predictions)

    return lrm_accuracy



def svm_model_df1(**kwargs):
    
    from sklearn.model_selection import train_test_split
    from sklearn.svm import SVC
    from sklearn.metrics import accuracy_score
    ti = kwargs['ti']
    df_fe1 = ti.xcom_pull(task_ids='feature_eng_1')
    X = df_fe1.drop(columns=['target'])
    y = df_fe1['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    svm = SVC()
    svm.fit(X_train, y_train)
    svm_predictions = svm.predict(X_test)
    svm1_accuracy = accuracy_score(y_test, svm_predictions)

    return svm1_accuracy


def svm_model_df2(**kwargs):
    
    from sklearn.model_selection import train_test_split
    from sklearn.svm import SVC
    from sklearn.metrics import accuracy_score
    ti = kwargs['ti']
    df_fe2 = ti.xcom_pull(task_ids='feature_eng_2')

    X = df_fe2.drop(columns=['target'])
    y = df_fe2['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    svm = SVC()
    svm.fit(X_train, y_train)
    svm_predictions = svm.predict(X_test)
    svm2_accuracy = accuracy_score(y_test, svm_predictions)

    return svm2_accuracy

def svm_model_dfmerge(**kwargs):

    from sklearn.model_selection import train_test_split
    from sklearn.svm import SVC
    from sklearn.metrics import accuracy_score
    ti = kwargs['ti']
    df_merge = ti.xcom_pull(task_ids='merge')
    X = df_merge.drop(columns=['target'])
    y = df_merge['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    svm = SVC()
    svm.fit(X_train, y_train)
    svm_predictions = svm.predict(X_test)
    svmm_accuracy = accuracy_score(y_test, svm_predictions)

    return svmm_accuracy


def best_model(**kwargs):
    ti = kwargs['ti']

    lr1_accuracy = ti.xcom_pull(task_ids='lr_model_df1')
    lr2_accuracy = ti.xcom_pull(task_ids='lr_model_df2')
    lrm_accuracy = ti.xcom_pull(task_ids='lr_model_dfmerge')
    svm1_accuracy = ti.xcom_pull(task_ids='svm_model_df1')
    svm2_accuracy = ti.xcom_pull(task_ids='svm_model_df2')
    svmm_accuracy = ti.xcom_pull(task_ids='svm_model_dfmerge')

    accuracies = [
        ("lr_model_df1", lr1_accuracy),
        ("lr_model_df2", lr2_accuracy),
        ("lr_model_dfmerge", lrm_accuracy),
        ("svm_model_df1", svm1_accuracy),
        ("svm_model_df2", svm2_accuracy),
        ("svm_model_dfmerge", svmm_accuracy)
    ]

    print(accuracies)

    best_accuracy = max(accuracies, key=lambda x: x[1])

    return best_accuracy, f"The best model is {best_accuracy[0]} with an accuracy of {best_accuracy[1]}"



def evaluate_on_test(**kwargs):
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.svm import SVC
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
    import pandas as pd
    

    # Retrieve the best model information from XCom
    ti = kwargs['ti']
    best_model_info, best_model_message = ti.xcom_pull(task_ids='best_model')

    best_model_name = best_model_info[0]
    print(f"Evaluating the best model: {best_model_name}")

    if best_model_name == 'lr_model_df1':
        df_fe1 = ti.xcom_pull(task_ids='feature_eng_1')
        X = df_fe1.drop(columns=['target'])
        y = df_fe1['target']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        lr = LogisticRegression()
        lr.fit(X_train, y_train)
        predictions = lr.predict(X_test)
    elif best_model_name == 'lr_model_df2':
        df_fe2 = ti.xcom_pull(task_ids='feature_eng_2')
        X = df_fe2.drop(columns=['target'])
        y = df_fe2['target']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        lr = LogisticRegression()
        lr.fit(X_train, y_train)
        predictions = lr.predict(X_test)
    elif best_model_name == 'lr_model_dfmerge':
        df_merge = ti.xcom_pull(task_ids='merge')
        X = df_merge.drop(columns=['target'])
        y = df_merge['target']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        lr = LogisticRegression()
        lr.fit(X_train, y_train)
        predictions = lr.predict(X_test)
    elif best_model_name == 'svm_model_df1':
        df_fe1 = ti.xcom_pull(task_ids='feature_eng_1')
        X = df_fe1.drop(columns=['target'])
        y = df_fe1['target']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        svm = SVC()
        svm.fit(X_train, y_train)
        predictions = svm.predict(X_test)
    elif best_model_name == 'svm_model_df2':
        df_fe2 = ti.xcom_pull(task_ids='feature_eng_2')
        X = df_fe2.drop(columns=['target'])
        y = df_fe2['target']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        svm = SVC()
        svm.fit(X_train, y_train)
        predictions = svm.predict(X_test)
    elif best_model_name == 'svm_model_dfmerge':
        df_merge = ti.xcom_pull(task_ids='merge')
        X = df_merge.drop(columns=['target'])
        y = df_merge['target']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        svm = SVC()
        svm.fit(X_train, y_train)
        predictions = svm.predict(X_test)

# Calculate evaluation metrics
    accuracy = accuracy_score(y_test, predictions)
    precision = precision_score(y_test, predictions, average='binary')
    recall = recall_score(y_test, predictions, average='binary')
    f1 = f1_score(y_test, predictions, average='binary')

    # Print evaluation metrics
    print(f"Accuracy of the best model on the testing data: {accuracy}")
    print(f"Precision of the best model on the testing data: {precision}")
    print(f"Recall of the best model on the testing data: {recall}")
    print(f"F1 Score of the best model on the testing data: {f1}")

    # Return evaluation metrics
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
    }
    


    

    


# Instantiate the DAG
dag = DAG(
    'HW4',
    default_args=default_args,
    description='EDA with feature engineering and model selection',
    schedule_interval=PARAMS['workflow']['workflow_schedule_interval'],
    tags=["de300"]
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

clean_impute_sklearn_task = PythonOperator(
    task_id='clean_impute_sklearn',
    python_callable=clean_impute_sklearn,
    provide_context=True,
    dag=dag
)

feature_eng_1_task = PythonOperator(
    task_id='feature_eng_1',
    python_callable=feature_eng_1,
    provide_context=True,
    dag=dag
)


clean_impute_spark_task = PythonOperator(
    task_id='clean_impute_spark',
    python_callable=clean_impute_spark,
    provide_context=True,
    dag=dag
)

feature_eng_2_task = PythonOperator(
    task_id='feature_eng_2',
    python_callable=feature_eng_2,
    provide_context=True,
    dag=dag
)


web_scrape_task = PythonOperator(
    task_id='web_scrape',
    python_callable=web_scrape,
    provide_context=True,
    dag=dag
)

merge_task = PythonOperator(
    task_id='merge',
    python_callable=merge,
    provide_context=True,
    dag=dag
)


lr1_task = PythonOperator(
    task_id='lr_model_df1',
    python_callable=lr_model_df1,
    provide_context=True,
    dag=dag
)


lr2_task = PythonOperator(
    task_id='lr_model_df2',
    python_callable=lr_model_df2,
    provide_context=True,
    dag=dag
)

lrmerge_task = PythonOperator(
    task_id='lr_model_dfmerge',
    python_callable=lr_model_dfmerge,
    provide_context=True,
    dag=dag
)


svm1_task = PythonOperator(
    task_id='svm_model_df1',
    python_callable=svm_model_df1,
    provide_context=True,
    dag=dag
)


svm2_task = PythonOperator(
    task_id='svm_model_df2',
    python_callable=svm_model_df2,
    provide_context=True,
    dag=dag
)


svm_merge_task = PythonOperator(
    task_id='svm_model_dfmerge',
    python_callable=svm_model_dfmerge,
    provide_context=True,
    dag=dag
)


best_model_task = PythonOperator(
    task_id='best_model',
    python_callable=best_model,
    provide_context=True,
    dag=dag
)


evaluate_on_test_task = PythonOperator(
    task_id='evaluate_on_test',
    python_callable=evaluate_on_test,
    provide_context=True,
    dag=dag
)

load_data_task >> [clean_impute_sklearn_task, clean_impute_spark_task] 
clean_impute_sklearn_task >> feature_eng_1_task >> [lr1_task, svm1_task, merge_task]
clean_impute_spark_task >> feature_eng_2_task >> [lr2_task, svm2_task, merge_task]
web_scrape_task >> merge_task >> [lrmerge_task, svm_merge_task]
[lr1_task, svm1_task,lr2_task, svm2_task,lrmerge_task, svm_merge_task] >> best_model_task
best_model_task >> evaluate_on_test_task