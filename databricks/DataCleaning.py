"""
This python file was ran within a Databricks notebook.
It is used within the batch_data_queries to clean the data read from the S3 bucket
"""
from pyspark.sql.functions import col, concat, to_timestamp, array, regexp_replace, expr

class DataCleaning:
    
    @staticmethod
    def clean_user_data(df):
        """
        The clean_user_data function takes in a DataFrame and returns a cleaned version of the same DataFrame.
        The cleaning process includes:
            - Dropping any duplicate records
            - Adding a username column in place of the first &amp; last names
            - Dropping the previously concatenated name columns (first_name, last_name) 
            - Transforming the timestamp column from string to timestamp datatype.  This is done by using Spark's built-in to_timestamp function.   The date format used is 'yyyy-MM-dd HH:mm:ss' as specified by ISO 8601 standard for
        
        :param df: Pass the dataframe to be cleaned
        :return: A dataframe
        :doc-author: Trelent
        """
        # Creating a copy to show the stage of cleaning
        df_user_cleaning = df

        # Dropping any duplicate records
        df_user_cleaning = df_user_cleaning.dropDuplicates()

        # Adding a username column in place of the first & last names
        df_user_cleaning = df_user_cleaning.withColumn("username", concat("first_name", "last_name"))

        # Dropping the previously concatenated name columns
        cols_to_drop = ['first_name', 'last_name']
        df_user_cleaning = df_user_cleaning.drop(*cols_to_drop)

        # Transforming the timestamp column from a string to timestamp datatype.
        df_user_cleaning = df_user_cleaning.withColumn("date_joined", to_timestamp("date_joined"))

        # Renaming of index to ind
        df_user_cleaning = df_user_cleaning.withColumnRenamed("index", "ind")

        # Reordering columns
        revised_col_order = [
            "ind",
            "username",
            "age",
            "date_joined"
        ]
        df_user_cleaning = df_user_cleaning.select(revised_col_order)
        
        # Creation of new variable to store the cleaned DataFrame
        cleaned_df_user = df_user_cleaning

        return cleaned_df_user
    
    @staticmethod
    def clean_geo_data(df):
        """
        The clean_geo_data function takes in a DataFrame and returns a cleaned version of the same DataFrame.
        The cleaning process includes:
            - Dropping any duplicate rows
            - Condensing the coordinates by making an array
            - Dropping the latitude and longitude columns (since they are now part of an array) 
            - Transforming the timestamp column from a string to timestamp datatype.  This is done so that it can be used for time series analysis later on.  
                The original data type was string, which would not allow us to do this kind of analysis with it.  
        
        :param df: Pass in the dataframe that is being cleaned
        :return: A dataframe with the following columns:
        """
        # Creating a copy to show the stage of cleaning
        df_geo_cleaning = df

        # Dropping any duplicate rows
        df_geo_cleaning = df_geo_cleaning.dropDuplicates()

        # Condensing the coordinates by making an array
        df_geo_cleaning = df_geo_cleaning.withColumn("coordinates", array("latitude", "longitude"))

        # Dropping the latitude and longitude columns
        cols_to_drop = ["latitude", "longitude"]
        df_geo_cleaning = df_geo_cleaning.drop(*cols_to_drop)

        # Transforming the timestamp column from a string to timestamp datatype.
        df_geo_cleaning = df_geo_cleaning.withColumn("timestamp", to_timestamp("timestamp"))

        # Renaming of index to ind
        df_geo_cleaning = df_geo_cleaning.withColumnRenamed("index", "ind")
        # Reordering columns
        revised_col_order = [
            "ind",
            "country",
            "coordinates",
            "timestamp"
        ]
        df_geo_cleaning = df_geo_cleaning.select(revised_col_order)

        # Creation of new variable to store the cleaned DataFrame
        cleaned_df_geo = df_geo_cleaning

        return cleaned_df_geo
    
    @staticmethod
    def clean_pin_data(df):
        """
        The clean_pin_data function takes in a DataFrame and returns a cleaned version of the same DataFrame.
        The cleaning process includes:
            - Dropping duplicates from the original dataframe.
            - Replacing unretrieved data with NULL values for specific columns. 
                This is done by using a dictionary to store key-value pairs, where each key represents the column name and its value represents what needs to be replaced with NULL values (if applicable). The function then iterates through this dictionary, replacing any rows that match these conditions with NULL values instead of their original value(s). 
        
        :param df: Pass in the dataframe that is being cleaned
        :return: The cleaned dataframe
        """
        # Creating a copy to show the stage of cleaning
        df_pin_cleaning = df

        # Dropping duplicates
        df_pin_cleaning = df_pin_cleaning.dropDuplicates()

        # Replacing unretrieved data with NULL values
        for_incorrect_rows = {'description': 'No description available',
                      'follower_count': 'User Info Error',
                      'image_src': 'Image src error.',
                      'poster_name': 'User Info Error',
                      'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e',
                      'title': 'No Title Data Available'}

        for key, value in for_incorrect_rows.items():
            df_pin_cleaning = df_pin_cleaning.withColumn(
            key, expr(f"CASE WHEN {key} LIKE '{value}%' THEN NULL ELSE {key} END")
            )
        
        # Replacing `k` and `M` with its respective amount of 0s for 'kilo' and 'million'
        df_pin_cleaning = df_pin_cleaning.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
        df_pin_cleaning = df_pin_cleaning.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
        # Column can now be cast to an `int` dtype
        df_pin_cleaning = df_pin_cleaning.withColumn("follower_count", col("follower_count").cast('int'))

        # Clean the data in the save_location column to include only the save location path
        df_pin_cleaning = df_pin_cleaning.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

        # Rename the index column to ind.
        df_pin_cleaning = df_pin_cleaning.withColumnRenamed("index", "ind")

        # Reordering the columns
        revised_col_order = [
            "ind",
            "unique_id",
            "title",
            "description",
            "follower_count",
            "poster_name",
            "tag_list",
            "is_image_or_video",
            "image_src",
            "save_location",
            "category"
        ]
        df_pin_cleaning = df_pin_cleaning.select(revised_col_order)

        # Creation of new variable to store the cleaned DataFrame
        clean_df_pin = df_pin_cleaning

        return clean_df_pin