import boto3
from datetime import datetime

# Pure function to create a Kinesis client
def create_kinesis_client():
    return boto3.client('kinesis')

# Pure function to get the shard iterator from a specific timestamp
def get_shard_iterator_at_timestamp(kinesis_client, stream_name, shard_id, timestamp):
    response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='AT_TIMESTAMP',  # Specify the iterator type
        Timestamp=timestamp  # Pass the specific timestamp
    )
    return response['ShardIterator']

# Pure function to retrieve records using a shard iterator
def get_records(kinesis_client, shard_iterator, limit=100):
    response = kinesis_client.get_records(
        ShardIterator=shard_iterator,
        Limit=limit
    )
    return response['Records'], response['NextShardIterator']

# Main function that manages the process
def main():
    print('Starting script')
    
    stream_name = 'real-time-food-data'
    shard_id = 'shardId-000000000000'  # Replace with your actual shard ID

    # Define the specific timestamp (UTC)
    timestamp = datetime(2024, 9, 14, 1, 9, 0)  # 15th September 2024, 01:09:00 UTC

    kinesis_client = create_kinesis_client()
    
    # Get a shard iterator for a specific timestamp
    shard_iterator = get_shard_iterator_at_timestamp(kinesis_client, stream_name, shard_id, timestamp)
    
    # Retrieve records from the specific time frame
    records, next_shard_iterator = get_records(kinesis_client, shard_iterator)
    
    # Print the fetched records
    print(records)

if __name__ == "__main__":
    main()
