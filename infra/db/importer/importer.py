import os
import gzip
import json
import psycopg2
import boto3
from io import BytesIO
from botocore import UNSIGNED
from botocore.client import Config

def connect_db():
    return psycopg2.connect(
        host=os.environ['DATABASE_HOST'],
        port=os.environ['DATABASE_PORT'],
        dbname=os.environ['DATABASE_NAME'],
        user=os.environ['DATABASE_USER'],
        password=os.environ['DATABASE_PASSWORD']
    )

def create_tables(cursor):
    # Create tables for different data types
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mapping_data (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mapping_data_v2 (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
            year INT,
            game_id TEXT,
            data JSONB
        );
    """)

def import_data(cursor, table_name, json_data, additional_values=None):
    if table_name == 'games' and additional_values:
        cursor.execute(
            "INSERT INTO games (year, game_id, data) VALUES (%s, %s, %s)",
            (additional_values['year'], additional_values['game_id'], json.dumps(json_data))
        )
    else:
        cursor.execute(
            f"INSERT INTO {table_name} (data) VALUES (%s)",
            [json.dumps(json_data)]
        )
        print("Printing data,")
        print(json_data)

def process_file(s3_client, bucket_name, key, cursor):
    print(f"Processing {key}")
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    compressed_body = response['Body'].read()
    with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
        content = gz.read().decode('utf-8')
        json_data = json.loads(content)

        if key.endswith('leagues.json.gz'):
            import_data(cursor, 'leagues', json_data)
        elif key.endswith('mapping_data.json.gz'):
            import_data(cursor, 'mapping_data', json_data)
        elif key.endswith('mapping_data_v2.json.gz'):
            import_data(cursor, 'mapping_data_v2', json_data)
        elif key.endswith('players.json.gz'):
            import_data(cursor, 'players', json_data)
        elif key.endswith('teams.json.gz'):
            import_data(cursor, 'teams', json_data)
        elif key.endswith('tournaments.json.gz'):
            import_data(cursor, 'tournaments', json_data)
        elif 'games/' in key:
            # Skip for now
            return
            # Extract year and game_id from the key
            parts = key.split('/')
            year = int(parts[2])
            game_filename = parts[3]
            game_id = game_filename.replace('.json.gz', '').replace('val:', '')
            additional_values = {'year': year, 'game_id': game_id}
            import_data(cursor, 'games', json_data, additional_values)
        else:
            print(f"Unrecognized file: {key}")

def main():
    # AWS S3 configuration
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    s3_bucket_prefix = os.environ.get('S3_BUCKET_PREFIX', '')

    # Initialize S3 client with anonymous access
    s3_client = boto3.client(
        's3',
        region_name='us-west-2',
        config=Config(signature_version=UNSIGNED)
    )

    # Connect to the database
    conn = connect_db()
    cursor = conn.cursor()
    create_tables(cursor)
    conn.commit()

    # List and process files from S3
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=s3_bucket_name, Prefix=s3_bucket_prefix)

    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            try:
                process_file(s3_client, s3_bucket_name, key, cursor)
                conn.commit()
            except Exception as e:
                print(f"Error processing {key}: {e}")
                conn.rollback()

    # Close the database connection
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()

