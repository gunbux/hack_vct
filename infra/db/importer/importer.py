import os
import time
import gzip
import json
import mysql.connector
from io import BytesIO
from datetime import datetime
from botocore import UNSIGNED
from botocore.client import Config
import boto3

def connect_db(retries=5, delay=5):
    for attempt in range(retries):
        try:
            conn = mysql.connector.connect(
                host=os.environ['DATABASE_HOST'],
                port=int(os.environ['DATABASE_PORT']),
                database=os.environ['DATABASE_NAME'],
                user=os.environ['DATABASE_USER'],
                password=os.environ['DATABASE_PASSWORD'],
                auth_plugin='mysql_native_password'
            )
            print("Connected to MySQL")
            return conn
        except mysql.connector.Error as e:
            print(f"Connection failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Exceeded maximum retries. Exiting.")
                raise

def create_tables(cursor):
    # Create leagues table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            league_id VARCHAR(50) PRIMARY KEY,
            region VARCHAR(50),
            dark_logo_url TEXT,
            light_logo_url TEXT,
            name VARCHAR(100),
            slug VARCHAR(100)
        );
    """)

    # Create teams table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id VARCHAR(50) PRIMARY KEY,
            acronym VARCHAR(10),
            home_league_id VARCHAR(50),
            dark_logo_url TEXT,
            light_logo_url TEXT,
            slug VARCHAR(100),
            name VARCHAR(100)
        );
    """)

    # Create players table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id VARCHAR(50) PRIMARY KEY,
            handle VARCHAR(50),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            status VARCHAR(20),
            photo_url TEXT,
            home_team_id VARCHAR(50),
            created_at DATETIME,
            updated_at DATETIME
        );
    """)

    # Create mapping_data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mapping_data (
            platformGameId VARCHAR(100) PRIMARY KEY,
            esportsGameId VARCHAR(50),
            tournamentId VARCHAR(50),
            teamMapping JSON,
            participantMapping JSON
        );
    """)

    # Create mapping_data_v2 table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mapping_data_v2 (
            platformGameId VARCHAR(100) PRIMARY KEY,
            matchId VARCHAR(50),
            esportsGameId VARCHAR(50),
            tournamentId VARCHAR(50),
            teamMapping JSON,
            participantMapping JSON
        );
    """)

    # Create tournaments table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            id VARCHAR(50) PRIMARY KEY,
            status VARCHAR(20),
            league_id VARCHAR(50),
            time_zone VARCHAR(50),
            name VARCHAR(100)
        );
    """)

    # Create games table with composite unique key
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INT AUTO_INCREMENT PRIMARY KEY,
            platformGameId VARCHAR(100),
            includedPauses VARCHAR(20),
            year INT,
            metadata JSON,
            snapshot JSON,
            INDEX idx_platformGameId (platformGameId),
            UNIQUE KEY uniq_platformGameId_includedPauses (platformGameId, includedPauses)
        );
    """)

def parse_datetime(dt_str):
    """
    Converts ISO 8601 datetime string to MySQL DATETIME format.
    Example: '2021-05-13T22:37:14Z' -> '2021-05-13 22:37:14'
    """
    if not dt_str:
        return None
    try:
        # Handle fractional seconds if present
        if '.' in dt_str:
            dt = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            dt = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as ve:
        print(f"ValueError: {ve} for datetime string: {dt_str}")
        return None
    except TypeError as te:
        print(f"TypeError: {te} for datetime string: {dt_str}")
        return None

def import_data(cursor, table_name, json_data, year=None):
    if table_name == 'players':
        created_at = parse_datetime(json_data.get('created_at'))
        updated_at = parse_datetime(json_data.get('updated_at'))
        cursor.execute(
            """
            INSERT INTO players (id, handle, first_name, last_name, status, photo_url, home_team_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                handle = VALUES(handle),
                first_name = VALUES(first_name),
                last_name = VALUES(last_name),
                status = VALUES(status),
                photo_url = VALUES(photo_url),
                home_team_id = VALUES(home_team_id),
                created_at = VALUES(created_at),
                updated_at = VALUES(updated_at);
            """,
            (
                json_data.get('id'),
                json_data.get('handle'),
                json_data.get('first_name'),
                json_data.get('last_name'),
                json_data.get('status'),
                json_data.get('photo_url'),
                json_data.get('home_team_id'),
                created_at,
                updated_at
            )
        )
    elif table_name == 'teams':
        cursor.execute(
            """
            INSERT INTO teams (id, acronym, home_league_id, dark_logo_url, light_logo_url, slug, name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                acronym = VALUES(acronym),
                home_league_id = VALUES(home_league_id),
                dark_logo_url = VALUES(dark_logo_url),
                light_logo_url = VALUES(light_logo_url),
                slug = VALUES(slug),
                name = VALUES(name);
            """,
            (
                json_data.get('id'),
                json_data.get('acronym'),
                json_data.get('home_league_id'),
                json_data.get('dark_logo_url'),
                json_data.get('light_logo_url'),
                json_data.get('slug'),
                json_data.get('name')
            )
        )
    elif table_name == 'leagues':
        cursor.execute(
            """
            INSERT INTO leagues (league_id, region, dark_logo_url, light_logo_url, name, slug)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                region = VALUES(region),
                dark_logo_url = VALUES(dark_logo_url),
                light_logo_url = VALUES(light_logo_url),
                name = VALUES(name),
                slug = VALUES(slug);
            """,
            (
                json_data.get('league_id'),
                json_data.get('region'),
                json_data.get('dark_logo_url'),
                json_data.get('light_logo_url'),
                json_data.get('name'),
                json_data.get('slug')
            )
        )
    elif table_name == 'mapping_data':
        cursor.execute(
            """
            INSERT INTO mapping_data (platformGameId, esportsGameId, tournamentId, teamMapping, participantMapping)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                esportsGameId = VALUES(esportsGameId),
                tournamentId = VALUES(tournamentId),
                teamMapping = VALUES(teamMapping),
                participantMapping = VALUES(participantMapping);
            """,
            (
                json_data.get('platformGameId'),
                json_data.get('esportsGameId'),
                json_data.get('tournamentId'),
                json.dumps(json_data.get('teamMapping')),
                json.dumps(json_data.get('participantMapping'))
            )
        )
    elif table_name == 'mapping_data_v2':
        cursor.execute(
            """
            INSERT INTO mapping_data_v2 (platformGameId, matchId, esportsGameId, tournamentId, teamMapping, participantMapping)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                matchId = VALUES(matchId),
                esportsGameId = VALUES(esportsGameId),
                tournamentId = VALUES(tournamentId),
                teamMapping = VALUES(teamMapping),
                participantMapping = VALUES(participantMapping);
            """,
            (
                json_data.get('platformGameId'),
                json_data.get('matchId'),
                json_data.get('esportsGameId'),
                json_data.get('tournamentId'),
                json.dumps(json_data.get('teamMapping')),
                json.dumps(json_data.get('participantMapping'))
            )
        )
    elif table_name == 'tournaments':
        cursor.execute(
            """
            INSERT INTO tournaments (id, status, league_id, time_zone, name)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                league_id = VALUES(league_id),
                time_zone = VALUES(time_zone),
                name = VALUES(name);
            """,
            (
                json_data.get('id'),
                json_data.get('status'),
                json_data.get('league_id'),
                json_data.get('time_zone'),
                json_data.get('name')
            )
        )
    elif table_name == 'games':
        included_pauses = json_data.get('metadata', {}).get('eventTime', {}).get('includedPauses')
        print("including pauses:", included_pauses)
        cursor.execute(
            """
            INSERT INTO games (platformGameId, includedPauses, year, metadata, snapshot)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                year = VALUES(year),
                metadata = VALUES(metadata),
                snapshot = VALUES(snapshot);
            """,
            (
                json_data.get('platformGameId'),
                included_pauses,
                year,
                json.dumps(json_data.get('metadata')),
                json.dumps(json_data.get('snapshot'))
            )
        )
    else:
        print(f"Unrecognized table: {table_name}")

def process_file(s3_client, bucket_name, key, cursor):
    print(f"Processing {key}")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        compressed_body = response['Body'].read()
        with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
            content = gz.read().decode('utf-8')
            try:
                json_data = json.loads(content)
            except json.JSONDecodeError as jde:
                print(f"JSONDecodeError for {key}: {jde}")
                return

            if key.endswith('players.json.gz'):
                if isinstance(json_data, list):
                    for player in json_data:
                        import_data(cursor, 'players', player)
                elif isinstance(json_data, dict):
                    if 'id' in json_data:
                        import_data(cursor, 'players', json_data)
                    else:
                        for player in json_data.values():
                            import_data(cursor, 'players', player)
                else:
                    print("Unexpected data format in players.json.gz")
            elif key.endswith('teams.json.gz'):
                if isinstance(json_data, list):
                    for team in json_data:
                        import_data(cursor, 'teams', team)
                elif isinstance(json_data, dict):
                    if 'id' in json_data:
                        import_data(cursor, 'teams', json_data)
                    else:
                        for team in json_data.values():
                            import_data(cursor, 'teams', team)
                else:
                    print("Unexpected data format in teams.json.gz")
            elif key.endswith('leagues.json.gz'):
                if isinstance(json_data, list):
                    for league in json_data:
                        import_data(cursor, 'leagues', league)
                elif isinstance(json_data, dict):
                    if 'league_id' in json_data:
                        import_data(cursor, 'leagues', json_data)
                    else:
                        for league in json_data.values():
                            import_data(cursor, 'leagues', league)
                else:
                    print("Unexpected data format in leagues.json.gz")
            elif key.endswith('mapping_data.json.gz'):
                if isinstance(json_data, list):
                    for mapping in json_data:
                        import_data(cursor, 'mapping_data', mapping)
                elif isinstance(json_data, dict):
                    import_data(cursor, 'mapping_data', json_data)
                else:
                    print("Unexpected data format in mapping_data.json.gz")
            elif key.endswith('mapping_data_v2.json.gz'):
                if isinstance(json_data, list):
                    for mapping in json_data:
                        import_data(cursor, 'mapping_data_v2', mapping)
                elif isinstance(json_data, dict):
                    import_data(cursor, 'mapping_data_v2', json_data)
                else:
                    print("Unexpected data format in mapping_data_v2.json.gz")
            elif key.endswith('tournaments.json.gz'):
                if isinstance(json_data, list):
                    for tournament in json_data:
                        import_data(cursor, 'tournaments', tournament)
                elif isinstance(json_data, dict):
                    if 'id' in json_data:
                        import_data(cursor, 'tournaments', json_data)
                    else:
                        for tournament in json_data.values():
                            import_data(cursor, 'tournaments', tournament)
                else:
                    print("Unexpected data format in tournaments.json.gz")
            elif 'games/' in key and key.endswith('.json.gz'):
                # Extract year and game_id from the key
                try:
                    parts = key.split('/')
                    if len(parts) < 4:
                        print(f"Invalid game file path: {key}")
                        return
                    year = int(parts[2])
                    game_filename = parts[3]
                    if isinstance(json_data, list):
                        for game in json_data:
                            import_data(cursor, 'games', game, year=year)
                    elif isinstance(json_data, dict):
                        import_data(cursor, 'games', json_data, year=year)
                    else:
                        print(f"Unexpected data format in {key}")
                except (IndexError, ValueError) as e:
                    print(f"Error extracting year and game_id from {key}: {e}")
            else:
                print(f"Unrecognized file: {key}")
    except Exception as e:
        print(f"Error processing {key}: {e}")

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

    # Connect to the database with retry logic
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

