from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import TransformationLayer

cwd = os.getcwd()


spark = SparkSession.builder \
    .appName('Logistics Company ETL') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hudi:hudi-utilities-bundle_2.12:0.13.1,org.apache.spark:spark-avro_2.13:3.4.0,org.apache.calcite:calcite-core:1.34.0') \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config('spark.driver.memory', '16g') \
    .getOrCreate()

iot_data = spark.read.format('csv').option('header','true').load(f'file:///{cwd}/data/case_study/iot_device_data.csv')
orders_data = spark.read.format('csv').option('header','true').load(f'file:///{cwd}/data/case_study/order_tracking_data.csv')

iot_data.show()
orders_data.show()

print(iot_data.dtypes)
print(orders_data.dtypes)

# Converting String Datetime to Date
iot_data = iot_data.withColumn("timestamp", to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data = orders_data.withColumn("order_date", to_date(col('order_date'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data = orders_data.withColumn("delivery_start_datetime", to_timestamp(col('delivery_start_datetime'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data = orders_data.withColumn("delivery_end_datetime", to_timestamp(col('delivery_end_datetime'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))

# Converting String Float to Float Dtype
cols_to_convert = ['latitude', 'longitude', 'fuel_level', 'engine_temperature', 'battery_temperature', 'speed']

for colC in cols_to_convert:
    iot_data = iot_data.withColumn(colC, col(colC).cast(FloatType()))

print(iot_data.dtypes)

cols_to_convert = ['delivery_latitude', 'delivery_longitude']

for colC in cols_to_convert:
    orders_data = orders_data.withColumn(colC, col(colC).cast(FloatType()))

print(orders_data.dtypes)

pending_orders_data = orders_data.filter(orders_data.order_status == 'pending')
pending_orders_data.show()

# Merging both the data sets to create a single dataframe
joined_df = orders_data.join(iot_data, orders_data.vehicle_id == iot_data.vehicle_id)\
                             .filter((iot_data.timestamp >= orders_data.delivery_start_datetime) & 
                                     (iot_data.timestamp <= orders_data.delivery_end_datetime)).drop(iot_data.vehicle_id)

joined_df.show()

# Grouping the data to get a single record for each order by taking a mean of the iot values
aggregated_df = joined_df.groupBy(["order_id", "order_status", "order_date", "delivery_latitude", "delivery_longitude", "delivery_start_datetime", "delivery_end_datetime", "vehicle_id"]).agg(
    mean("fuel_level").alias("fuel_level"),
    mean("engine_temperature").alias("engine_temperature"),
    mean("battery_temperature").alias("battery_temperature"),
    mean("speed").alias("speed")
)

# Show the aggregated results
aggregated_df.show()

# Transforming data into Hudi Tables
transformation = TransformationLayer(spark)

# Setting Hudi Options for Bronze Layer

# raw_vehicle_partition
transformation.set_hoodie_options(
    'raw_vehicle_partition',
    'order_id',
    'order_date',
    'vehicle_id,order_status,order_date'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/bronze_layer/raw_vehicle_partition',
    aggregated_df
)

# raw_status_partition
transformation.set_hoodie_options(
    'raw_status_partition',
    'order_id',
    'order_date',
    'order_status,order_date,vehicle_id'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/bronze_layer/raw_status_partition',
    aggregated_df
)

# raw_date_partition
transformation.set_hoodie_options(
    'raw_date_partition',
    'order_id',
    'order_date',
    'order_date,order_status,vehicle_id'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/bronze_layer/raw_date_partition',
    aggregated_df
)

# pending_raw_date_partition
transformation.set_hoodie_options(
    'pending_raw_date_partition',
    'order_id',
    'order_date',
    'order_date,order_status,vehicle_id'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/bronze_layer/pending_raw_date_partition',
    pending_orders_data
)

# Registering bronze_layer tables in pyspark
transformation.read_hoodie_table('raw_vehicle_partition', f'{cwd}/data/case_study/bronze_layer/raw_vehicle_partition')
transformation.read_hoodie_table('raw_status_partition', f'{cwd}/data/case_study/bronze_layer/raw_status_partition')
transformation.read_hoodie_table('raw_date_partition', f'{cwd}/data/case_study/bronze_layer/raw_date_partition')
transformation.read_hoodie_table('pending_raw_date_partition', f'{cwd}/data/case_study/bronze_layer/pending_raw_date_partition')

# ----------- SILVER LAYER ------------ #


# Orders Table
orders = transformation.query_hudi_table(
    """
    SELECT
    order_id,
    order_date,
    order_status,
    vehicle_id,
    fuel_level,
    engine_temperature,
    battery_temperature,
    speed
    FROM raw_date_partition

    UNION

    SELECT
    order_id,
    order_date,
    order_status,
    NULL as vehicle_id,
    NULL as fuel_level,
    NULL as engine_temperature,
    NULL as battery_temperature,
    NULL as speed
    FROM pending_raw_date_partition
    """
)

transformation.set_hoodie_options(
    'orders',
    'order_id',
    'order_date',
    'order_date,order_status,vehicle_id'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/silver_layer/orders',
    orders
)
transformation.read_hoodie_table('orders', f'{cwd}/data/case_study/silver_layer/orders')


# ml_orders Table
ml_orders = transformation.query_hudi_table(
    """
    SELECT
    order_id,
    order_date,
    order_status,
    vehicle_id,
    fuel_level,
    engine_temperature,
    battery_temperature,
    speed,
    delivery_start_datetime,
    delivery_end_datetime
    FROM raw_date_partition
    WHERE order_status = 'delivered'
    """
)

transformation.set_hoodie_options(
    'ml_orders',
    'order_id',
    'order_date',
    'order_date,order_status,vehicle_id'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/silver_layer/ml_orders',
    ml_orders
)
transformation.read_hoodie_table('ml_orders', f'{cwd}/data/case_study/silver_layer/ml_orders')

# compliance_orders Table
compliance_orders = transformation.query_hudi_table(
    """
    SELECT
    order_id,
    order_date,
    order_status,
    vehicle_id,
    fuel_level,
    engine_temperature,
    battery_temperature,
    speed,
    delivery_start_datetime,
    delivery_end_datetime
    FROM raw_date_partition
    WHERE order_status in ('delivered', 'in transit')
    """
)

transformation.set_hoodie_options(
    'compliance_orders',
    'order_id',
    'order_date',
    'order_date,order_status,vehicle_id'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/silver_layer/compliance_orders',
    compliance_orders
)
transformation.read_hoodie_table('compliance_orders', f'{cwd}/data/case_study/silver_layer/compliance_orders')


# ----------- GOLD LAYER ------------ #

# Sales Dashboard Table
sales_dashboard = transformation.query_hudi_table(
    """
    SELECT
    order_date,
    order_status,
    COUNT(DISTINCT(order_id)) as order_count,
    COUNT(DISTINCT(vehicle_id)) as vehicle_count
    FROM orders
    GROUP BY 1,2
    """
)

transformation.set_hoodie_options(
    'sales_dashboard',
    'order_date',
    'order_date',
    'order_date,order_status'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/gold_layer/sales_dashboard',
    sales_dashboard
)
transformation.read_hoodie_table('sales_dashboard', f'{cwd}/data/case_study/gold_layer/sales_dashboard')

# Pending Orders Table
pending_orders_dashboard = transformation.query_hudi_table(
    """
    SELECT
    order_date,
    order_id,
    order_status,
    vehicle_id
    FROM orders
    WHERE order_status in ('pending', 'in transit')
    """
)

transformation.set_hoodie_options(
    'pending_orders_dashboard',
    'order_id',
    'order_date',
    'order_date,order_status'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/gold_layer/pending_orders_dashboard',
    pending_orders_dashboard
)
transformation.read_hoodie_table('pending_orders_dashboard', f'{cwd}/data/case_study/gold_layer/pending_orders_dashboard')

# Compliance Check Table
compliance_check = transformation.query_hudi_table(
    """
    SELECT
    order_date,
    vehicle_id,
    CASE
        WHEN AVG(fuel_level) >= 40 THEN true
        ELSE false
    END as fuel_compliant,
    CASE
        WHEN (AVG(engine_temperature) > 40) OR (AVG(battery_temperature) > 30) THEN false
        ELSE true
    END as temperature_compliant,
    CASE
        WHEN AVG(speed) > 50 THEN false
        ELSE true
    END as speed_compliant
    FROM compliance_orders
    GROUP BY 1,2
    """
)

transformation.set_hoodie_options(
    'compliance_check',
    'order_date',
    'order_date',
    'order_date'
)
transformation.write_to_hudi_table(
    f'{cwd}/data/case_study/gold_layer/compliance_check',
    compliance_check
)
transformation.read_hoodie_table('compliance_check', f'{cwd}/data/case_study/gold_layer/compliance_check')