"""
This python file was ran within a Databricks notebook.
It is used within the batch_data_queries to create the raw DataFrame.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PinterestPipeline").getOrCreate()

def load_s3_data(directory):
    """
    The load_s3_data function takes a directory name as an argument and returns a Spark DataFrame.
    The function uses the SparkSession object to read in data from S3, inferring the schema of each column.
    
    
    :param directory: Specify the directory in s3 that we want to load data from
    :return: A dataframe
    """
    file_location = f"/mnt/0e4a38902653_mount/topics/{directory}/partition=0/*.json"
    file_type = "json"
    infer_schema = "true"
    df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .load(file_location)
    return df