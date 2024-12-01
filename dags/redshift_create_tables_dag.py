from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator


default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('create_and_load_dim',
         default_args=default_args,
         description='ETL for food delivery data into Redshift',
         start_date=days_ago(1),
         schedule_interval=None,
         catchup=False,
         tags=['redshift_operation']
) as dag:
    
    #Create schema if it doesn't exist
    create_schema = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id='redshift_connection_id',
        sql = "CREATE SCHEMA IF NOT EXISTS food_delivery_datamart;",
    )

# Drop tables with CASCADE to handle dependencies
    drop_dimCustomers = PostgresOperator(
        task_id='drop_dimCustomers_table',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.dimCustomers CASCADE;",
    )

    drop_dimDeliveryRiders = PostgresOperator(
        task_id='drop_dimDelivery_Riders_table',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.dimDeliveryRiders CASCADE;",
    )

    drop_dimRestaurants = PostgresOperator(
        task_id='drop_dimRestaurants_table',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.dimRestaurants CASCADE;",
    )

    drop_factOrders = PostgresOperator(
        task_id='drop_factOrders_table',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.factOrders CASCADE;",
    )


    # Create dimension and fact tables

    create_dimCustomers = PostgresOperator(
        task_id = 'create_dimCustomers_table',
        postgres_conn_id = 'redshift_connection_id',
        sql = """CREATE TABLE food_delivery_datamart.dimCustomers (
        CustomerID INT Primary KEY,
        CustomerName VARCHAR(255),
        CustomerEmail VARCHAR(255),
        CustomerPhone VARCHAR(50),
        CustomerAddress VARCHAR(500),
        RegistrationDate DATE
        );
        """,
    )


    create_dimRestaurants = PostgresOperator(
        task_id = 'create_dimRestaurants',
        postgres_conn_id = 'redshift_connection_id',
        sql="""
            CREATE TABLE food_delivery_datamart.dimRestaurants (
            RestaurantID INT PRIMARY KEY,
            RestaurantName VARCHAR(255),
            CuisineType VARCHAR(100),
            RestaurantAddress VARCHAR(500),
            RestaurantRating DECIMAL(3,1)
            );
            """,
    )

    create_dimDeliveryRiders = PostgresOperator(
        task_id = 'create_dimDeliveryRiders_table',
        postgres_conn_id = 'redshift_connection_id',
        sql="""
            CREATE TABLE food_delivery_datamart.dimDeliveryRiders (
            RiderID INT PRIMARY KEY,
            RiderName VARCHAR(255),
            RiderPhone VARCHAR(50),
            RiderVehicleType VARCHAR(50),
            VehicleID VARCHAR(50),
            RiderRating DECIMAL(3,1)
            );
            """,
    )

    create_factOrders = PostgresOperator(
        task_id = 'create_factOrders',
        postgres_conn_id = 'redshift_connection_id',
        sql = """
              CREATE TABLE food_delivery_datamart.factOrders (
              OrderID INT PRIMARY KEY,
              CustomerID INT REFERENCES food_delivery_datamart.dimCustomers(CustomerID),
              RestaurantID INT REFERENCES food_delivery_datamart.dimRestaurants(RestaurantID),
              RiderID INT REFERENCES food_delivery_datamart.dimDeliveryRiders(RiderID),
              OrderDate TIMESTAMP WITHOUT TIME ZONE,
              DeliveryTime INT,
              OrderValue DECIMAL(8,2),
              DeliveryFee DECIMAL(8,2),
              TipAmount DECIMAL(8,2),
              OrderStatus VARCHAR(50)
              );  
              """,
    )

    # Load data into dimension tables from S3

    load_dimCustomers = S3ToRedshiftOperator(
        task_id = 'load_data_in_dimCustomers_table',
        schema = 'food_delivery_datamart',
        table = 'dimCustomers',
        s3_bucket = 'dimensional-data-for-load',
        s3_key = 'dimCustomers.csv',
        copy_options = ['CSV','IGNOREHEADER 1','QUOTE as \'"\''],
        aws_conn_id = 'aws_default',
        redshift_conn_id = 'redshift_connection_id',
    )

    load_dimRestaurants = S3ToRedshiftOperator(
        task_id = 'load_data_in_dimRestaurants_table',
        schema = 'food_delivery_datamart',
        table = 'dimRestaurants',
        s3_bucket = 'dimensional-data-for-load',
        s3_key = 'dimRestaurants.csv',
        copy_options =['CSV','IGNOREHEADER 1','QUOTE as \'"\''],
        aws_conn_id = 'aws_default',
        redshift_conn_id = 'redshift_connection_id',
    )

    load_dimDeliveryRiders = S3ToRedshiftOperator(
        task_id = 'load_data_in_dimDeliveryRiders_table',
        schema = 'food_delivery_datamart',
        table = 'dimDeliveryRiders',
        s3_bucket = 'dimensional-data-for-load',
        s3_key = 'dimDeliveryRiders.csv',
        copy_options = ['CSV','IGNOREHEADER 1','QUOTE as \'"\''],
        aws_conn_id = 'aws_default',
        redshift_conn_id= 'redshift_connection_id',
    )


    trigger_spark_streaming_dag = TriggerDagRunOperator(
        task_id = 'trigger_spark_streaming_dag',
        trigger_dag_id = "submit_pyspark_streaming_job_to_emr",
    )

# First, create the schema
create_schema >> [drop_dimCustomers, drop_dimRestaurants, drop_dimDeliveryRiders, drop_factOrders]

# Once the existing tables are dropped, proceed to create new tables
drop_dimCustomers >> create_dimCustomers
drop_dimRestaurants >> create_dimRestaurants
drop_dimDeliveryRiders >> create_dimDeliveryRiders
drop_factOrders >> create_factOrders

[create_dimCustomers, create_dimRestaurants, create_dimDeliveryRiders] >> create_factOrders

# After each table is created, load the corresponding data
create_dimCustomers >> load_dimCustomers
create_dimRestaurants >> load_dimRestaurants
create_dimDeliveryRiders >> load_dimDeliveryRiders

[load_dimCustomers, load_dimRestaurants, load_dimDeliveryRiders] >> trigger_spark_streaming_dag