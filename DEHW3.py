# Import Libraries

from pyspark.sql.functions import col, when, lit, avg, stddev, log1p
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import re
import urllib.request

# Initialize Spark Session

spark = SparkSession.builder \
    .appName("HeartDiseasePrediction") \
    .getOrCreate()

# Load Data from S3

df = spark.read.csv("s3://de300-lily-bucket/heart_disease(in).csv", header=True, inferSchema=True)
df = df.limit(899)

# Data Cleaning and Transformation...

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

# Add new columns smokebyage and smokebysex to the table HeartDisease2
df = df.withColumn('smokesource1', lit(None).cast('double'))
df = df.withColumn('smokesource2', lit(None).cast('double'))


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


# Update DataFrame with smoking rates from source 1
for age_to_smoke_rate in smoking_rates:
    for age, smoking_rate in age_to_smoke_rate.items():
        df = df.withColumn('smokesource1', when(col('age') == age, lit(smoking_rate)).otherwise(col('smokesource1')))
if 'senior_age' in locals() and 'senior_smoke_rate' in locals():
    df = df.withColumn('smokesource1', when(col('age') >= senior_age, lit(senior_smoke_rate)).otherwise(col('smokesource1')))

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

from pyspark.sql.functions import when

# Function to update DataFrame with smoking rates based on age groups
def update_smoking_rates(df, age_start, age_end, smoking_rate):
    return df.withColumn('smokesource2', 
                         when((col('sex') == 0) & (col('age') >= age_start) & (col('age') <= age_end), smoking_rate)
                         .otherwise(col('smokesource2')))

# Age group 1
age_start1, age_end1 = int(group1_data[2]), int(group1_data[3])
smoking_rate1 = float(group1_data[4])
df = update_smoking_rates(df, age_start1, age_end1, smoking_rate1)

# Age group 2
age_start2, age_end2 = int(group2_data[2]), int(group2_data[3])
smoking_rate2 = float(group2_data[4])
df = update_smoking_rates(df, age_start2, age_end2, smoking_rate2)

# Age group 3
age_start3, age_end3 = int(group3_data[2]), int(group3_data[3])
smoking_rate3 = float(group3_data[4])
df = update_smoking_rates(df, age_start3, age_end3, smoking_rate3)

# Age group 4
age_start4 = int(group4_data[2])
smoking_rate4 = float(group4_data[3])
df = df.withColumn('smokesource2', 
                   when((col('sex') == 0) & (col('age') >= age_start4), smoking_rate4)
                   .otherwise(col('smokesource2')))


# Extract relevant lines containing smoke rates for men and women
block_list_gender_text = re.findall(r'<ul class="block-list">(.*?)</ul>', html2, re.DOTALL)[0]

# Use regular expressions to extract lines containing smoke rates for men and women
men_smoke_rate = float(re.findall(r'<li class="main">.*men \((.*?)%\)</li>', block_list_gender_text)[0])
women_smoke_rate = float(re.findall(r'<li class="main">.*women \((.*?)%\)</li>', block_list_gender_text)[0])

gender_ratio = men_smoke_rate / women_smoke_rate


men_smoke1 = (float(group1_data[4]))* gender_ratio
men_smoke2 = (float(group2_data[4]))* gender_ratio
men_smoke3 = (float(group3_data[4]))* gender_ratio
men_smoke4 = (float(group4_data[3]))* gender_ratio

# Function to update DataFrame with smoking rates for males based on age groups
def update_smoking_rates_males(df, age_start, age_end, smoking_rate):
    return df.withColumn('smokesource2', 
                         when((col('sex') == 1) & (col('age') >= age_start) & (col('age') <= age_end), smoking_rate)
                         .otherwise(col('smokesource2')))

# Age group 1
age_start1, age_end1 = int(group1_data[2]), int(group1_data[3])
df = update_smoking_rates_males(df, age_start1, age_end1, men_smoke1)

# Age group 2
age_start2, age_end2 = int(group2_data[2]), int(group2_data[3])
df = update_smoking_rates_males(df, age_start2, age_end2, men_smoke2)

# Age group 3
age_start3, age_end3 = int(group3_data[2]), int(group3_data[3])
df = update_smoking_rates_males(df, age_start3, age_end3, men_smoke3)

# Age group 4
age_start4 = int(group4_data[2])
df = df.withColumn('smokesource2', 
                   when((col('sex') == 1) & (col('age') >= age_start4), men_smoke4)
                   .otherwise(col('smokesource2')))


# Calculate average of smokesource1 and smokesource2 columns and replace smoke column
df = df.withColumn('smoke', (col('smokesource1') + col('smokesource2')) / 2)

# Drop smokesource1 and smokesource2 columns
df = df.drop('smokesource1', 'smokesource2')


# Select the desired columns
selected_columns = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 
                    'prop', 'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 
                    'oldpeak', 'slope', 'target']



# Filter the DataFrame to retain only the selected columns
df = df.select(*selected_columns)


# Import necessary libraries
from pyspark.sql.functions import col, when, lit, avg, stddev, log1p
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Split the data into training and test sets with stratification on labels
trainData, testData = df.randomSplit([0.9, 0.1], seed=12345)

from pyspark.ml.feature import VectorAssembler
feature_cols = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope']
trainData = trainData.withColumn('age', col('age').cast('double'))
testData = testData.withColumn('age', col('age').cast('double'))

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
trainData = assembler.transform(trainData)
testData = assembler.transform(testData)

# Define the binary classifier models
lr = LogisticRegression(labelCol="target")
rf = RandomForestClassifier(labelCol="target")
dt = DecisionTreeClassifier(labelCol="target")

# Define the parameter grids for hyperparameter tuning
lr_paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

rf_paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10, 20]) \
    .addGrid(rf.numTrees, [50, 100, 200]) \
    .build()

dt_paramGrid = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [5, 10, 15]) \
    .build()

# List of models and their corresponding parameter grids
models = [lr, rf, dt]
paramGrids = [lr_paramGrid, rf_paramGrid, dt_paramGrid]

# Define the evaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", metricName="areaUnderROC", labelCol="target")

# Set the parallelism level (adjust as needed)
num_cores = spark.sparkContext.defaultParallelism
parallelism = num_cores * 3  # Adjust the level of parallelism as needed

best_model = None
best_auc = 0.0
best_model_name = ""

# Train the models with cross-validation and hyperparameter tuning
for model, paramGrid in zip(models, paramGrids):
    crossval = CrossValidator(estimator=model,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator,
                              numFolds=5,
                              parallelism=parallelism)
    cvModel = crossval.fit(trainData)
    predictions = cvModel.transform(testData)
    auc = evaluator.evaluate(predictions)
    model_name = model.__class__.__name__

    print(f"Model: {model_name}, AUC: {auc}")

    if auc > best_auc:
        best_auc = auc
        best_model = cvModel.bestModel
        best_model_name = model_name

print(f"Best Model: {best_model_name}, Best AUC: {best_auc}")