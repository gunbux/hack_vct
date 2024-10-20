import boto3
from botocore import UNSIGNED
from botocore.config import Config
import requests
import gzip
import json
from io import BytesIO
import pandas as pd
from game_cleaning import GameDataCleaner  # Assuming GameDataCleaner is in a module
from agg import PlayerPerformanceAggregator
import os

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
prefix = f'{LEAGUE}/'

# Get the list of JSON files from S3 (excluding unwanted files)
game_json_files = list_s3_objects(prefix)

# Function to read the last processed batch from the log file
def read_last_processed_batch(log_file='progress_log.txt'):
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            return int(f.read().strip())
    return 0  # If no log file exists, start from the first batch

# Function to write the current (next) batch number to the log file
def write_last_processed_batch(batch_number, log_file='progress_log.txt'):
    # Increment batch_number to reflect the next batch
    with open(log_file, 'w') as f:
        f.write(str(batch_number + 1))  # Write the next batch to process


# Function to process a batch of files
def process_batch(batch_files, batch_number, total_files):
    try:
        for i, game_file in enumerate(batch_files, start=batch_number * batch_size + 1):
            # Load each game file from S3
            json_data = load_gz_file_from_s3(game_file)

            if json_data:
                # Pass the loaded JSON data to GameDataCleaner
                team_pf, round_df, player_pf = GameDataCleaner.genGameDataFromJson(json_data)

                # Skip if the game is a draw (team_pf, round_df, player_pf are None)
                if team_pf is None and round_df is None and player_pf is None:
                    print(f"Skipped game file {game_file} due to a draw.")
                    continue  # Skip to the next file
                
                # Aggregate the player performance data
                aggregator.aggregate_player_data(player_pf)

            print(f"Processed file {i}/{total_files}: {game_file}")
        
        # Save the aggregated data after processing the batch
        aggregator.save_agg_df()
        print(f"Batch {batch_number} processed successfully and saved.")
        
        # Log the next batch number to the progress log
        write_last_processed_batch(batch_number)

    except Exception as e:
        print(f"Error processing batch {batch_number}: {e}")
        raise  # Re-raise the exception to stop processing




# Define the list of files to exclude
excluded_files = [
    'vct-international/esports-data/leagues.json.gz',
    'vct-international/esports-data/mapping_data.json.gz',
    'vct-international/esports-data/mapping_data_v2.json.gz',
    'vct-international/esports-data/players.json.gz',
    'vct-international/esports-data/teams.json.gz',
    'vct-international/esports-data/tournaments.json.gz'
]

# Filter out the excluded files from game_json_files
filtered_game_json_files = [file for file in game_json_files if file not in excluded_files]

# Create an instance of PlayerPerformanceAggregator
aggregator = PlayerPerformanceAggregator()

# Batch size for processing
batch_size = 50
total_files = len(filtered_game_json_files)

# Read the last successfully processed batch from the log file
last_processed_batch = read_last_processed_batch()

# Loop over the filtered game JSON files in batches, resuming from the last successful batch
if filtered_game_json_files:
    for batch_number in range(last_processed_batch, (total_files + batch_size - 1) // batch_size):
        batch_start = batch_number * batch_size
        batch_end = min(batch_start + batch_size, total_files)
        current_batch = filtered_game_json_files[batch_start:batch_end]
        
        # Process the current batch
        process_batch(current_batch, batch_number, total_files)
else:
    print("No valid game JSON files found.")

