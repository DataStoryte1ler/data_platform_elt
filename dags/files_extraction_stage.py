import boto3
import os

def upload_local_files_to_s3(file_path, bucket_name, s3_key):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the file to S3 at the specified key
    s3.upload_file(file_path, bucket_name, s3_key)

    print(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'.")


# Specify the list of local file paths and their corresponding S3 folder structure
file_mappings = [
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/customers.csv',
        's3_key': 'csv_data/customers.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/geolocation.csv',
        's3_key': 'csv_data/geolocation.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/order_items.csv',
        's3_key': 'csv_data/order_items.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/order_payments.csv',
        's3_key': 'csv_data/order_payments.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/order_reviews.csv',
        's3_key': 'csv_data/order_reviews.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/orders.csv',
        's3_key': 'csv_data/orders.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/products.csv',
        's3_key': 'csv_data/products.csv'
    },
    {
        'local_path': '/home/yaroslav/development/data_engineering/data_platform/raw_data/sellers.csv',
        's3_key': 'csv_data/sellers.csv'
    }
]

# S3 bucket name
bucket_name = 'data-platform-raw-data-storage'

def elt_files():    
    # Upload each local file to the corresponding folder in the S3 bucket
    for file_mapping in file_mappings:
        local_path = file_mapping['local_path']
        s3_key = file_mapping['s3_key']
        upload_local_files_to_s3(local_path, bucket_name, s3_key)

elt_files()