#!/bin/bash

# Usage: ./run_ml_pipeline.sh s3://path/to/input.csv s3://path/to/output_model

INPUT_PATH=$1
OUTPUT_PATH=$2

spark-submit --master yarn \
	     --deploy-mode cluster \
	     modifiedfunc.py $INPUT_PATH $OUTPUT_PATH

