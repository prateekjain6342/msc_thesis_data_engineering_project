import os
import time
import datetime
from utils import HudiRead
from pyspark.sql.functions import *
from pyspark.sql.types import *

start_time = time.time()

hudi = HudiRead()
cwd = os.getcwd()
# --------------- Business Requirement 1: Total Order Count for D-1 ------------------ #

#### TRADITIONAL APPROACH
# Reading base data in pyspark and merging
orders_data = hudi.read_csv(f"{cwd}/data/case_study/order_tracking_data.csv")
print(orders_data)

# Order count as on D-1 with segregation of all types of order_status
orders_data = orders_data.withColumn("order_date", to_date(col('order_date'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data.registerTempTable('tr_orders_data')
data = hudi.query_hudi_table(
    """
    SELECT
    order_status,
    COUNT(DISTINCT(order_id)) as order_count
    FROM tr_orders_data
    WHERE order_date = '2024-03-17'
    GROUP BY 1
    """
)
print(data)
print(f"Time Taken: {time.time() - start_time}")


### METHODOLOGY APPROACH
hudi.read_hoodie_table("sales_dashboard", f'{cwd}/data/case_study/gold_layer/sales_dashboard')
data = hudi.query_hudi_table(
    """
    SELECT
    order_status,
    order_count
    FROM sales_dashboard
    WHERE order_date = '2024-03-17'
    """
)
print(data)
print(f"Time Taken: {time.time() - start_time}")

# --------------- Business Requirement 2: All Delivered Orders for Data Science Team ------------------ #

#### TRADITIONAL APPROACH
# Reading base data in pyspark and merging
orders_data = hudi.read_csv(f"{cwd}/data/case_study/order_tracking_data.csv")
iot_data = hudi.read_csv(f"{cwd}/data/case_study/iot_device_data.csv")

# Order count as on D-1 with segregation of all types of order_status
iot_data = iot_data.withColumn("timestamp", to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data = orders_data.withColumn("order_date", to_date(col('order_date'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data = orders_data.withColumn("delivery_start_datetime", to_timestamp(col('delivery_start_datetime'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
orders_data = orders_data.withColumn("delivery_end_datetime", to_timestamp(col('delivery_end_datetime'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))

# Merging both the data sets to create a single dataframe of delivered orders
joined_df = orders_data.join(iot_data, orders_data.vehicle_id == iot_data.vehicle_id)\
                             .filter((iot_data.timestamp >= orders_data.delivery_start_datetime) & 
                                     (iot_data.timestamp <= orders_data.delivery_end_datetime) & (orders_data.order_status == 'delivered')).drop(iot_data.vehicle_id)


joined_df.registerTempTable('tr_ds_orders')
data = hudi.query_hudi_table(
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
    FROM tr_ds_orders
    """
)
print(data)
print(f"Time Taken: {time.time() - start_time}")


#### METHODOLOGY APPROACH
hudi.read_hoodie_table("ml_orders", f'{cwd}/data/case_study/silver_layer/ml_orders')
data = hudi.query_hudi_table(
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
    FROM ml_orders
    """
)
print(data)
print(f"Time Taken: {time.time() - start_time}")