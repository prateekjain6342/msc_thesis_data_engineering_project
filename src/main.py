import pandas as pd
import json
from utils import NormaliseIoT

def get_iot_data():
    data = json.load(open('./data/iot_data.json', 'r'))
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
