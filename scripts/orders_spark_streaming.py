from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
import argparse

parser = argparse.ArgumentParser(description="Pyspark Streaming Job Arguments")
parser.add_argument('--redshift_user', required=True, help='Redshift Username')
parser.add_argument('--redshift_password',required=True,help='Redshift Password')
parser.add_argument('--aws_access_key',required=True, help='aws_access_key')
parser.add_argument('--aws_secret_key',required=True,help='aws_secret_key')
args = parser.parse_args()


