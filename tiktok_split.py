import argparse
import sys
import logging
import time
import boto3  
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from py4j.java_gateway import java_import

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize boto3 S3 client
s3_client = boto3.client('s3')

# Simulate command-line arguments for testing
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument('--i', type=str, help='An input file argument --i', required=True)
    parser.add_argument('--t', type=str, help='A temporary folder argument --t', required=True)
    parser.add_argument('--o', type=str, help='An output folder argument --o', required=True)

    # Parse the arguments
    args = parser.parse_args()

    # Extract arguments
    input_file = args.i
    temp_folder = args.t
    output_folder = args.o

    logging.info(f"Input file provided: {input_file}")
    logging.info(f"Temporary folder provided: {temp_folder}")
    logging.info(f"Output folder provided: {output_folder}")

    # Initialize Spark session
    spark = SparkSession.builder.appName("TikTokUsageSplit").getOrCreate()

    # Define temporary paths based on the provided temp_folder argument
    temp_creations_path = temp_folder.rstrip('/') + "/temp_creations/"
    temp_views_path = temp_folder.rstrip('/') + "/temp_views/"

    # Define output paths based on input file's naming conventions and the provided output_folder argument
    base_output_path = output_folder.rstrip('/') + "/"
    creations_output_file = base_output_path + input_file.split('/')[-1].replace(".gz", "_creations.csv")
    views_output_file = base_output_path + input_file.split('/')[-1].replace(".gz", "_views.csv")

    # Extract bucket name and prefixes for boto3 operations
    bucket_name = temp_folder.split('/')[2]  # Extract the bucket name from the S3 path
    creations_prefix = "/".join(temp_creations_path.split('/')[3:])  # Remove the "s3://<bucket_name>/" part
    views_prefix = "/".join(temp_views_path.split('/')[3:])
    creations_output_prefix = "/".join(creations_output_file.split('/')[3:])
    views_output_prefix = "/".join(views_output_file.split('/')[3:])

    # Import Java classes needed for file operations
    sc = spark.sparkContext
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")

    # Function to move files in S3 using boto3
    def move_s3_file(source_bucket, source_key, dest_bucket, dest_key):
        try:
            # Copy the file to the new location
            s3_client.copy_object(Bucket=dest_bucket, CopySource={'Bucket': source_bucket, 'Key': source_key}, Key=dest_key)
            # Delete the original file
            s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            logging.info(f"File written to : {dest_bucket}/{dest_key}")
        except Exception as e:
            logging.error(f"Failed to move file from {source_bucket}/{source_key} to {dest_bucket}/{dest_key}: {str(e)}")
            raise

    # Function to delete temporary folders in S3
    def delete_s3_folder(bucket_name, prefix):
        try:
            # List all objects in the folder
            objects_to_delete = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' in objects_to_delete:
                # Delete each object
                delete_keys = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_keys})
                # logging.info(f"Deleted folder and its contents at {bucket_name}/{prefix}")
        except Exception as e:
            logging.error(f"Failed to delete folder {bucket_name}/{prefix}: {str(e)}")
            raise

    try:
        # Read input data from the .gz file with the correct encoding
        df = spark.read.option("header", "true") \
                       .option("delimiter", "|") \
                       .option("compression", "gzip") \
                       .option("encoding", "UTF-16") \
                       .csv(input_file)
        logging.info("Data read successfully from input file.")

        # Check for necessary columns
        required_columns = {'Creations', 'Views'}  # Ensure columns are in lowercase for consistency
        df_columns = set(df.columns)  # Use df.columns to get the list of column names in the DataFrame

        if not required_columns.issubset(df_columns):
            raise ValueError(f"Input file is missing required columns: {required_columns - df_columns}")

        # Trim and clean column names
        df = df.select([trim(col(c)).alias(c.strip()) for c in df.columns])

        # Filter for Creations: Creations not NULL or 0
        creations_df = df.filter((col("Creations").isNotNull()) & (col("Creations") != 0))

        # Filter for Views: Views not NULL or 0 and not included in Creations
        views_df = df.filter((col("Views").isNotNull()) & (col("Views") != 0) & ((col("Creations").isNull()) | (col("Creations") == 0)))

        # Write the Creations DataFrame to a temporary directory with overwrite mode
        creations_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_creations_path)
        logging.info(f"Creations file temporarily saved to: {temp_creations_path}")

        # Write the Views DataFrame to a temporary directory with overwrite mode
        views_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_views_path)
        logging.info(f"Views file temporarily saved to: {temp_views_path}")

        # Wait for S3 to become consistent
        time.sleep(10)

        # List files in the temporary directories using boto3
        creations_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=creations_prefix)
        views_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=views_prefix)

        # Identify the actual CSV part files from the listings
        creations_part_file = next(
            (content['Key'] for content in creations_files.get('Contents', []) if content['Key'].endswith('.csv')), None)
        views_part_file = next(
            (content['Key'] for content in views_files.get('Contents', []) if content['Key'].endswith('.csv')), None)

        if not creations_part_file:
            raise FileNotFoundError(f"No part CSV file found in {temp_creations_path}")
        if not views_part_file:
            raise FileNotFoundError(f"No part CSV file found in {temp_views_path}")

        # Move the part files to the desired output location
        move_s3_file(bucket_name, creations_part_file, bucket_name, creations_output_prefix)
        move_s3_file(bucket_name, views_part_file, bucket_name, views_output_prefix)

        # Delete temporary folders
        delete_s3_folder(bucket_name, creations_prefix)
        delete_s3_folder(bucket_name, views_prefix)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
