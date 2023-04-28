from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType, ArrayType
import json
from datetime import datetime

class search_in_sparksql(object):
    def __init__(self, search_string, table_name):
        # warehouse_location = "hdfs://data/"
        
        # Set the warehouse location
        warehouse_location = "file:///home/hx152/Project/tweet_project/Dataset"
        metastore_db_location = "/home/hx152/Project/tweet_project/Dataset/metastore_db"

        self.spark = SparkSession.builder \
            .enableHiveSupport() \
            .config("spark.sql.warehouse.dir", warehouse_location) \
            .config("spark.sql.legacy.createHiveTableByDefault", False) \
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_db_location};create=true") \
            .appName("UserInformation").getOrCreate()
        sc = self.spark.sparkContext
        self.sparksql = SQLContext(sc)
        
        self.fuzzy_search_result = self.fuzzy_search_by_name(search_string, table_name)
        self.exact_search_result = self.exact_search_by_name(search_string, table_name)
        
    def fuzzy_search_by_name(self, search_string, table_name):
        # Escape single quotes in the search string to avoid SQL injection
        search_string = search_string.replace("'", "''")

        # Execute the SQL query
        fuzzy_search_query = f"""
            SELECT *
            FROM {table_name}
            WHERE name LIKE '%{search_string}%' OR screen_name LIKE '%{search_string}%'
        """
        result = self.spark.sql(fuzzy_search_query)
        # Display the result
        return result
        
    def exact_search_by_name(self, search_string, table_name):
        # Escape single quotes in the search string to avoid SQL injection
        search_string = search_string.replace("'", "''")

        # Execute the SQL query
        exact_search_query = f"""
            SELECT *
            FROM {table_name}
            WHERE name = '%{search_string}%' OR screen_name = '%{search_string}%'
        """
        result = self.spark.sql(exact_search_query)   
        # Display the result
        return result
    
name = 'ondrisek'
table_name="user_data.new_user_beta"
ss = search_in_sparksql(name, table_name)
print("HERE IS THE RESULT FOR FUZZY SEARCH")
ss.fuzzy_search_result.show()
print("NEXT IS THE RESULT FOR EXACT SEARCH")
ss.exact_search_result.show()