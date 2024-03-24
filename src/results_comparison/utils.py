from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

class HudiRead:

    def __init__(self, spark_session = None) -> None:
        if spark_session is None:
            self.spark = SparkSession.builder \
                .appName('Hudi Data Reader') \
                .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hudi:hudi-utilities-bundle_2.12:0.13.1,org.apache.spark:spark-avro_2.13:3.4.0,org.apache.calcite:calcite-core:1.34.0') \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                .config('spark.driver.memory', '16g') \
                .getOrCreate()
        else:
            self.spark = spark_session

    def read_csv(self, file_path: str):
        data = self.spark.read.option('header','true').format('csv').load(f'file:///{file_path}')
        return data
        
    def read_hoodie_table(self, table_name: str, file_path: str):
        data = self.spark.read.format('org.apache.hudi').load(f'file:///{file_path}')
        data.registerTempTable(table_name)
        return data
    
    def query_hudi_table(self, query):
        data = self.spark.sql(query)
        data.show()
        return data
