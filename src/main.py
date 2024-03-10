import pandas as pd
import json
from utils import NormaliseIoT, NormaliseOrders
import datetime

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
            


if __name__ == "__main__":

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