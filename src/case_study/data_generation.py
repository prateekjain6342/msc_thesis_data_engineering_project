import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Parameters
num_days = 10
num_vehicles = 50
orders_per_day = 100000
iot_records_per_day_per_vehicle = int(24*60*60/5)  # Every 5 seconds for a day

# Generate Vehicle IDs
vehicle_ids = ['V'+str(i).zfill(5) for i in range(1, num_vehicles+1)]

# Helper function to generate random timestamps within a day
def random_timestamps(start, end, n):
    return [start + timedelta(seconds=x) for x in np.random.randint(0, int((end-start).total_seconds()), n)]

# IoT Data Generation
iot_data = []
for day in range(num_days):
    for vehicle_id in vehicle_ids:
        day_start = datetime.now() - timedelta(days=day)
        for _ in range(iot_records_per_day_per_vehicle):
            record = {
                'vehicle_id': vehicle_id,
                'timestamp': day_start,
                'latitude': np.random.uniform(-90, 90),
                'longitude': np.random.uniform(-180, 180),
                'fuel_level': np.random.uniform(0, 100),
                'engine_temperature': np.random.uniform(20, 120),
                'battery_temperature': np.random.uniform(20, 60),
                'speed': np.random.uniform(0, 120),
            }
            iot_data.append(record)
            day_start += timedelta(seconds=5)  # Increment by 5 seconds

iot_df = pd.DataFrame(iot_data)

# Order Tracking Data Generation
order_tracking_data = []
order_id = 1
for day in range(num_days):
    day_start = datetime.now() - timedelta(days=day)
    for _ in range(orders_per_day):
        order_status = np.random.choice(['pending', 'in transit', 'delivered'])
        order = {
            'order_id': 'O'+str(order_id).zfill(7),
            'order_status': order_status,
            'order_date': day_start,
            'delivery_latitude': np.random.uniform(-90, 90),
            'delivery_longitude': np.random.uniform(-180, 180),
        }

        if order_status != 'pending':
            vehicle_id = np.random.choice(vehicle_ids)
            delivery_start_datetime = day_start + timedelta(minutes=np.random.randint(1, 60))
            delivery_end_datetime = delivery_start_datetime + timedelta(minutes=np.random.randint(30, 120))
            order.update({
                'delivery_start_datetime': delivery_start_datetime,
                'delivery_end_datetime': delivery_end_datetime,
                'vehicle_id': vehicle_id,
            })
        else:
            # For pending orders, set these fields to None or an appropriate placeholder
            order.update({
                'delivery_start_datetime': None,
                'delivery_end_datetime': None,
                'vehicle_id': None,
            })

        order_tracking_data.append(order)
        order_id += 1

order_tracking_df = pd.DataFrame(order_tracking_data)

# Output the datasets to CSV files
iot_df.to_csv('./data/case_study/iot_device_data.csv', index=False)
order_tracking_df.to_csv('./data/case_study/order_tracking_data.csv', index=False)

# Display sample from each dataset
print(iot_df.head())
print(order_tracking_df.head())
