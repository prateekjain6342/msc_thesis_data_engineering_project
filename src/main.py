import pandas as pd
import json
from utils import NormaliseIoT, NormaliseOrders
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
import time

def get_iot_data():
    data = json.load(open('./data/iot_data.json', 'r'))
    return data

def get_orders_data():
    data = pd.read_csv('./data/orders_data.csv')
    return data

class PreProcessingLayer:

    @staticmethod
    def grouped_iot_data(iot_data: pd.DataFrame) -> pd.DataFrame:
        # Adding a date column
        iot_data['event_date'] = iot_data['timestamp'].dt.date

        grouped_data = iot_data.groupby(['vehicle_id', 'event_date']).agg({
            'battery_temperature': 'mean',
            'engine_temperature': 'mean',
            'speed': 'mean',
            'fuel_level': 'mean'
        }).reset_index()
        
        return grouped_data

    @staticmethod
    def get_delivery_end_time(orders_data: pd.DataFrame) -> pd.DataFrame:
        delivery_start_epoch_time = 1704085200

        final = list()
        for data in orders_data.to_dict('records'):
            data['delivery_end_epoch'] = delivery_start_epoch_time + data['delivery_time']
            data['delivery_end_time'] = datetime.datetime.fromtimestamp(data['delivery_end_epoch'])
            # To update the start_time for the next order
            delivery_start_epoch_time += data['delivery_time']
            final += [data]
        
        return pd.json_normalize(final)
    
    @staticmethod
    def get_merged_data(orders_data: pd.DataFrame, iot_data: pd.DataFrame) -> pd.DataFrame:

        final = list()
        for order in orders_data.to_dict('records'):
            delivery_end_time = order.get('delivery_end_time')
            delivery_start_time = delivery_end_time - datetime.timedelta(seconds=order.get('delivery_time'))
            filt_iot_data = iot_data[(iot_data['timestamp'] >= delivery_start_time) & (iot_data['timestamp'] <= delivery_end_time)]
            group_filt_iot = PreProcessingLayer.grouped_iot_data(filt_iot_data)
            order.update(group_filt_iot.to_dict('records')[0]) if len (group_filt_iot.to_dict('records')) > 0 else ''
            final += [order]

        return pd.json_normalize(final)
            

class TransformationLayer:

    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .appName('Hudi ETL Job') \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hudi:hudi-utilities-bundle_2.12:0.13.1,org.apache.spark:spark-avro_2.13:3.4.0,org.apache.calcite:calcite-core:1.34.0') \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config('spark.driver.memory', '16g') \
            .getOrCreate()

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
        
    def read_base_parquet(self, file_path: str, file_format: str):
        data = self.spark.read.format(file_format) \
            .load(f'file:///{file_path}')
        
        data.show()
        return data
    
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

    pass


if __name__ == "__main__":
    import os
    cwd = os.getcwd()

    iot_data = get_iot_data()
    
    # Normalising/Pre-processing the iot data to ensure correct formats
    normalised_data = list()
    for data in iot_data:
        tmp_data = NormaliseIoT(data).build()
        # Adding pseudo vehicle id for combining
        tmp_data['vehicle_id'] = 1
        normalised_data += [tmp_data]
    
    # Converting Data to Pandas Dataframe for further processing
    iot_data = pd.json_normalize(normalised_data)
    print(iot_data)

    orders_data = get_orders_data()
    
    normalised_orders = list()
    for data in orders_data.to_dict('records'):
        tmp_data = NormaliseOrders(data).build()
        # Adding pseudo vehicle id for combining
        tmp_data['vehicle_id'] = 1
        normalised_orders += [tmp_data]
    
    # Converting Data back to Pandas Dataframe for further processing
    orders_data = pd.json_normalize(normalised_orders)
    print(orders_data)

    grouped_iot = PreProcessingLayer.grouped_iot_data(iot_data)
    print(grouped_iot)

    orders_data = PreProcessingLayer.get_delivery_end_time(orders_data)
    print(orders_data)

    merged_data = PreProcessingLayer.get_merged_data(orders_data, iot_data)
    print(merged_data)
    # Saving the merged data as a parquet
    merged_data.to_parquet('./data/base.parquet', index=False, coerce_timestamps='ms')

    transformation_layer = TransformationLayer()
    bronze_base = transformation_layer.read_base_parquet(f'{cwd}/data/base.parquet', 'parquet')
    
    #Adding a dummy order_id to create a primary key
    w = Window().orderBy(lit('A'))
    bronze_base = bronze_base.withColumn("order_id", row_number().over(w))
    transformation_layer.set_hoodie_options("bronze_layer", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/bronze_layer", bronze_base)

    start_time = time.time()
    # Reading back the Apache Hudi table as a database in PySpark
    bronze_layer = transformation_layer.read_hoodie_table("bronze_layer", f"{cwd}/data/bronze_layer")
    transformation_layer.query_hudi_table('select * from bronze_layer where delivery_status="in transit"')
    print(time.time() - start_time)

    start_time = time.time()
    # Reading back the Base data table as a database in PySpark
    bronze_base.registerTempTable('base_data')
    transformation_layer.query_hudi_table('select * from base_data where delivery_status="in transit"')
    print(time.time() - start_time)


    # Starting the creation of silver layer
    transit_orders = transformation_layer.query_hudi_table('select * from bronze_layer where delivery_status in ("in transit", "pending")')
    transit_orders = transit_orders.na.drop()
    transformation_layer.set_hoodie_options("transit_orders", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/silver_layer/transit_orders", transit_orders)
    transformation_layer.read_hoodie_table("transit_orders", f"{cwd}/data/silver_layer/transit_orders")

    successful_orders = transformation_layer.query_hudi_table('select * from bronze_layer where delivery_status in ("delivered")')
    successful_orders = successful_orders.na.drop()
    transformation_layer.set_hoodie_options("successful_orders", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/silver_layer/successful_orders", successful_orders)
    transformation_layer.read_hoodie_table("successful_orders", f"{cwd}/data/silver_layer/successful_orders")

    # Starting the creation of gold layer
    # Assuming that today's date is 2024-01-01
    daily_orders = transformation_layer.query_hudi_table('select * from bronze_layer where order_date = "2024-01-01"')
    daily_orders = daily_orders.na.drop()
    transformation_layer.set_hoodie_options("daily_orders", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/gold_layer/daily_orders", daily_orders)
    transformation_layer.read_hoodie_table("daily_orders", f"{cwd}/data/gold_layer/daily_orders")

    speed_inefficiency = transformation_layer.query_hudi_table('select * from transit_orders where speed > 50')
    print(speed_inefficiency)
    speed_inefficiency = speed_inefficiency.na.drop()
    transformation_layer.set_hoodie_options("speed_inefficiency", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/gold_layer/speed_inefficiency", speed_inefficiency)
    transformation_layer.read_hoodie_table("speed_inefficiency", f"{cwd}/data/gold_layer/speed_inefficiency")
    
    finances = transformation_layer.query_hudi_table('select * from successful_orders')
    print(finances)
    finances = finances.na.drop()
    transformation_layer.set_hoodie_options("finances", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/gold_layer/finances", finances)
    transformation_layer.read_hoodie_table("finances", f"{cwd}/data/gold_layer/finances")

    temperature_alerts = transformation_layer.query_hudi_table('select * from bronze_layer where battery_temperature > 30 or engine_temperature > 40')
    print(temperature_alerts)
    temperature_alerts = temperature_alerts.na.drop()
    transformation_layer.set_hoodie_options("temperature_alerts", "order_id", "delivery_end_time", "vehicle_id,delivery_status")
    transformation_layer.write_to_hudi_table(f"{cwd}/data/gold_layer/temperature_alerts", temperature_alerts)
    transformation_layer.read_hoodie_table("temperature_alerts", f"{cwd}/data/gold_layer/temperature_alerts")