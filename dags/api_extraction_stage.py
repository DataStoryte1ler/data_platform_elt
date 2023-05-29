import requests
import json
import boto3
import os


# API URL
api_url = "https://asos2.p.rapidapi.com/countries/list"

# Query parameters
querystring = {"lang":"en-US"}

# Headers
headers = {
	"X-RapidAPI-Key": "some_key",
	"X-RapidAPI-Host": "asos2.p.rapidapi.com"
}

# S3 bucket name
bucket_name = 'data-platform-raw-data-storage'

# File Path and S3 Key
local_file_path = 'countries.json'
s3_key = 'json_data/countries.json'

def get_data_from_api(api_url, headers, querystring):
    response = requests.get(api_url, headers=headers, params=querystring)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error retrieving data from API. Status code: {response.status_code}")

def save_json_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file)

def upload_file_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)

def check_file_exists_in_s3(bucket_name, s3_key):
    s3 = boto3.client('s3')

    try:
        # Head the S3 object to check if it exists
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        print(f"File with key '{s3_key}' exists in S3 bucket '{bucket_name}'.")
        return True
    except:
        print(f"File with key '{s3_key}' does not exist in S3 bucket '{bucket_name}'.")
        return False
    
# Runs entire process
def api_elt():

    # Get data from API
    data = get_data_from_api(api_url, headers, querystring)
    # Save data as JSON file
    save_json_to_file(data, 'countries.json')
    # Upload file to S3 bucket
    upload_file_to_s3(local_file_path, bucket_name, s3_key)
    # Check if the file exists in the S3 bucket
    file_exists = check_file_exists_in_s3(bucket_name, s3_key)
    if file_exists:
        # File was successfully loaded into S3
        print("File upload validation passed.")
    else:
        # File was not found in S3
        print("File upload validation failed.")
    # Remove local file
    os.remove(local_file_path)

api_elt()
