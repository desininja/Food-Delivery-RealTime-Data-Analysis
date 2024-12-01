import boto3
import random 
import os
import sys
import pandas as pd
from datetime import datetime 
import json
import time 
from faker import Faker

print('starting script')
fake = Faker()
kinesis_client = boto3.client('kinesis')
stream_name = 'real-time-food-data'

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def send_data_kinesis(stream_name,mock_data):
    
    response = kinesis_client.put_record(
        StreamName=stream_name,  
        Data=json.dumps(mock_data),
        PartitionKey=str(mock_data['OrderID']))
    
    print(f"Sent order to Kinesis with Sequence Number: {response['SequenceNumber']}")

def get_all_ids():
    base_path = get_script_path()
    data_path = base_path+'/'+'data_for_dims'

    customers = pd.read_csv(data_path+'/dimCustomers.csv')
    customerIDs = customers['CustomerID'].to_list()
    
    riders = pd.read_csv(data_path+'/'+'dimDeliveryRiders.csv')
    riderIDs = riders['RiderID'].to_list()

    restaurants = pd.read_csv(data_path+'/'+'dimRestaurants.csv')
    restIDs = restaurants['RestaurantID'].to_list()

    return(customerIDs,riderIDs,restIDs)

def generate_data(orderID,customerID,riderID, restID):
    
    mock_data = {
        'OrderID' : orderID,
        'CustomerID': customerID,
        'RiderID' : riderID,
        'RestaurantID' : restID,
        'OrderDate' : fake.date_time_between(start_date = '-30d', end_date = 'now').isoformat(),
        'DeliveryTime' : random.randint(20,80),
        'OrderValue' : round(random.uniform(100,10000),2),
        "DeliveryFee" : round(random.uniform(10,100),2),
        "TipAmount" : round(random.uniform(0,100),2),
        "OrderStatus" : random.choice(['Delivered','Cancelled','On the way','Processing'])}

    return mock_data



if __name__ =='__main__':
    customerIDs, riderIDs, restIDs = get_all_ids()

    for orderID in range(5000,5100):
                                   
        customerID= random.choice(customerIDs)
        riderID = random.choice(riderIDs)
        restID =  random.choice(restIDs)

        mock_data=generate_data(orderID,customerID,riderID,restID)
        print(mock_data)
        send_data_kinesis(stream_name,mock_data)