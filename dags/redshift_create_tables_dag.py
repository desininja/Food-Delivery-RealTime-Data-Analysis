from airflow import DAG
from airflow.providers.amazon.aws.transfer.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
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

    #drop tables if they exist
    drop_dimCustomers = PostgresOperator(
        task_id = 'drop_dimCustomers_table',
        postgres_conn_id = 'redshift_connection_id',
        sql = "DROP TABLE IF EXISTS food_delivery_datamart.dimCustomers;",
    )

    drop_dimDeliveryRiders = PostgresOperator(
        task_id = 'drop_dimDelivery_Riders_table',
        postgres_conn_id ='redshfit_connection_id',
        sql = "DROP TABLE IF EXISTS food_delivery_datamart.dimDeliveryRiders;",
    )

    drop_dimRestaurants = PostgresOperator(
        task_id = 'drop_dimRestaurants_table',
        postgres_conn_id = 'redshift_connection_id',
        sql = "DROP TABLE IF EXISTS food_delivery_datamart.dimRestaurants;",
    )

    drop_factOrders = PostgresOperator(
        task_id = 'drop_factOrders_table',
        postgres_conn_id = 'redshift_connection_id',
        sql = "DROP TABLE IF EXISTS food_delivery_datamart.factOrders;",
    )

    # Create dimension and fact tables

    


























    
    


