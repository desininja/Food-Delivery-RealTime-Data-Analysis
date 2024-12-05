from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
import argparse
import traceback

try:
    # Read Arguments
    parser = argparse.ArgumentParser(description="Pyspark Streaming Job Arguments")
    parser.add_argument('--redshift_user', required=True, help='Redshift Username')
    parser.add_argument('--redshift_password', required=True, help='Redshift Password')
    parser.add_argument('--aws_access_key', required=True, help='AWS Access Key')
    parser.add_argument('--aws_secret_key', required=True, help='AWS Secret Key')
    args = parser.parse_args()

    # Configurations
    appName = 'KinesisToRedshift'
    kinesisStreamName = "real-time-food-data"
    kinesisRegion = "us-east-1"
    checkpointLocation = "s3://k-stream-checkpointing1/kinesisToRedshift/"
    redshiftJdbcUrl = f"jdbc:redshift://redshift-cluster-1.ctxisia4pstb.us-east-1.redshift.amazonaws.com:5439/dev"
    redshiftTable = "food_delivery_datamart.factorders"
    tempDir = "s3://pyspark-scripts-for-projects/temp-folder/"

    # Schema of incoming JSON data from Kinesis
    schema = StructType([
        StructField("OrderID", IntegerType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("RestaurantID", IntegerType(), True),
        StructField("RiderID", IntegerType(), True),
        StructField("OrderDate", TimestampType(), True),
        StructField("DeliveryTime", IntegerType(), True),
        StructField("OrderValue", DecimalType(8, 2), True),
        StructField("DeliveryFee", DecimalType(8, 2), True),
        StructField("TipAmount", DecimalType(8, 2), True),
        StructField("OrderStatus", StringType(), True)
    ])

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(appName) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")

    # Read Stream from Kinesis
    try:
        df = spark.readStream \
            .format('kinesis') \
            .option('streamName', kinesisStreamName) \
            .option('startingPosition', 'latest') \
            .option('region', kinesisRegion) \
            .option('endpointUrl', 'https://kinesis.us-east-1.amazonaws.com') \
            .option('awsAccessKeyId', args.aws_access_key) \
            .option('awsSecretKey', args.aws_secret_key) \
            .load()
        print("Successfully connected to Kinesis stream.")
    except Exception as e:
        print("Error while connecting to Kinesis stream:")
        traceback.print_exc()
        raise e

    # Parse Streamed Data
    try:
        parsed_df = df.selectExpr("CAST(data AS STRING)").select(from_json(col("data"), schema).alias("parsed_data")).select("parsed_data.*")
    except Exception as e:
        print("Error while parsing the streamed data:")
        traceback.print_exc()
        raise e

    # Perform Stateful Deduplication
    try:
        deduped_df = parsed_df.withWatermark("OrderDate", "10 minutes").dropDuplicates(["OrderID"])
    except Exception as e:
        print("Error while performing deduplication:")
        traceback.print_exc()
        raise e

    # Write to Redshift Function
    def write_to_redshift(batch_df, batch_id):
        try:
            print(f"Writing batch {batch_id} to Redshift...")
            batch_df.write \
                .format("jdbc") \
                .option("url", redshiftJdbcUrl) \
                .option("user", args.redshift_user) \
                .option("password", args.redshift_password) \
                .option("dbtable", redshiftTable) \
                .option("tempdir", tempDir) \
                .option("driver", "com.amazon.redshift.jdbc.Driver") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} successfully written to Redshift.")
        except Exception as e:
            print(f"Error while writing batch {batch_id} to Redshift:")
            traceback.print_exc()

    # Write Stream to Redshift
    try:
        query = deduped_df.writeStream \
            .foreachBatch(write_to_redshift) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", checkpointLocation) \
            .start()

        print("Streaming job running... Check logs for details.")
        query.awaitTermination()
    except Exception as e:
        print("Error while setting up the streaming query:")
        traceback.print_exc()
        raise e

except Exception as main_e:
    print("An error occurred during the job execution:")
    traceback.print_exc()
    raise main_e