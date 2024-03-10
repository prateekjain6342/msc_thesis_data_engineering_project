import pandas as pd
import json
from utils import NormaliseIoT, NormaliseOrders

def get_iot_data():
    data = json.load(open('./data/iot_data.json', 'r'))
    return data

def get_orders_data():
    data = pd.read_csv('./data/orders_data.csv')
    return data

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