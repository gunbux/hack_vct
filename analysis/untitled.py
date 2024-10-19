import boto3
from botocore import UNSIGNED
from botocore.config import Config
import requests
import gzip
import json
from io import BytesIO
import pandas as pd
from game_cleaning import GameDataCleaner  # Assuming GameDataCleaner is in a module

# Set up S3 client with unsigned configuration for public access
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-west-2')

# Define the bucket name and S3 bucket URL
bucket_name = 'vcthackathon-data'
S3_BUCKET_URL = "https://vcthackathon-data.s3.us-west-2.amazonaws.com"

# Specify the league and year to explore
LEAGUE = "vct-international"
YEAR = 2024

def list_s3_objects(prefix=''):
    """
    List objects in an S3 bucket given a prefix.
    """
    try:
        # List objects with the specified prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append(obj['Key'])
        else:
            print("No objects found with the given prefix.")
        return files
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []

def load_gz_file_from_s3(file_path):
    """
    Load a gzipped file from S3 and decompress it in memory.
    
    :param file_path: The path of the file in the S3 bucket.
    :return: JSON data of the decompressed file.
    """
    file_url = f"{S3_BUCKET_URL}/{file_path}"
    response = requests.get(file_url, stream=True)
    
    if response.status_code == 200:
        # Decompress the gzipped file in memory
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as gzipped_file:
            json_data = json.loads(gzipped_file.read().decode('utf-8'))
        return json_data
    else:
        print(f"Failed to load {file_path}. Status code: {response.status_code}")
        return None

# Prefix for the S3 path
prefix = f'{LEAGUE}/games/{YEAR}/'

# Get the list of JSON files from S3 (excluding unwanted files)
game_json_files = list_s3_objects(prefix)

# Example: Process only the first file (for demonstration purposes)
if game_json_files:
    # Load the first game file from S3
    json_data = load_gz_file_from_s3(game_json_files[0])

    if json_data:
        # Pass the loaded JSON data to GameDataCleaner
        team_pf, round_df, player_pf = GameDataCleaner.genGameDataFromJson(json_data)

        # Print the player performance DataFrame (as an example)
        print(player_pf.head())

else:
    print("No game JSON files found.")
