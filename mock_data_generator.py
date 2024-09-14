import boto3
import random 
import os
import sys
import pandas as pd

print('starting script')
kinesis_client = boto3.client('kinesis')

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))




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



if __name__ =='__main__':
    customerIDs, riderIDs, restIDs = get_all_ids()

    print(customerIDs)