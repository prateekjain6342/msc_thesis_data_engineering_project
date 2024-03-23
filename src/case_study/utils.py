from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

class TransformationLayer:

    def __init__(self, spark_session = None) -> None:
        if spark_session is None:
            self.spark = SparkSession.builder \
                .appName('Hudi ETL Job') \
                .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hudi:hudi-utilities-bundle_2.12:0.13.1,org.apache.spark:spark-avro_2.13:3.4.0,org.apache.calcite:calcite-core:1.34.0') \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                .config('spark.driver.memory', '16g') \
                .getOrCreate()
        else:
            self.spark = spark_session

    def set_hoodie_options(self, table_name: str, record_key:str, precombine_field: str, partition_field: str):
        self.hoodie_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.partitionpath.field': partition_field,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.table.name': table_name,
            'hoodie.deltastreamer.source.dfs.listing.max.fileid': '-1',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.enable.data.skipping': 'true',
            'hoodie.metadata.enable': 'true',
            'hoodie.metadata.index.column.stats.enable': 'true'
        }
    
    def write_to_hudi_table(self, target_path, data):
        data.write.format('org.apache.hudi') \
            .options(**self.hoodie_options) \
            .mode('overwrite') \
            .save(f'file:///{target_path}')
        
    def read_hoodie_table(self, table_name: str, file_path: str):
        data = self.spark.read.format('org.apache.hudi').load(f'file:///{file_path}')
        data.registerTempTable(table_name)
        return data
    
    def query_hudi_table(self, query):
        data = self.spark.sql(query)
        data.show()
        return data
