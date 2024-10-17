import json
import pandas as pd

class GameDataCleaner:
    AGENT_MAP = {'ADD6443A-41BD-E414-F6AD-E58D267F4E95': 'Jett',
                 'A3BFB853-43B2-7238-A4F1-AD90E9E46BCC': 'Reyna',
                 'F94C3B30-42BE-E959-889C-5AA313DBA261': 'Raze',
                 '7F94D92C-4234-0A36-9646-3A87EB8B5C89': 'Yoru',
                 'EB93336A-449B-9C1B-0A54-A891F7921D69': 'Phoenix',
                 'BB2A4828-46EB-8CD1-E765-15848195D751': 'Neon',
                 '5F8D3A7F-467B-97F3-062C-13ACF203C006': 'Breach',
                 '6F2A04CA-43E0-BE17-7F36-B3908627744D': 'Skye',
                 '320B2A48-4D9B-A075-30F1-1F93A9B638FA': 'Sova',
                 '601DBBE7-43CE-BE57-2A40-4ABD24953621': 'Kayo',
                 '1E58DE9C-4950-5125-93E9-A0AEE9F98746': 'Killjoy',
                 '117ED9E3-49F3-6512-3CCF-0CADA7E3823B': 'Cypher',
                 '569FDD95-4D10-43AB-CA70-79BECC718B46': 'Sage',
                 '22697A3D-45BF-8DD7-4FEC-84A9E28C69D7': 'Chamber',
                 '8E253930-4C05-31DD-1B6C-968525494517': 'Omen',
                 '9F0D8BA9-4140-B941-57D3-A7AD57C6B417': 'Brimstone',
                 '41FB69C1-4189-7B37-F117-BCAF1E96F1BF': 'Astra',
                 '707EAB51-4836-F488-046A-CDA6BF494859': 'Viper',
                 'DADE69B4-4F5A-8528-247B-219E5A1FACD6': 'Fade',
                 '95B78ED7-4637-86D9-7E41-71BA8C293152': 'Harbor',
                 'E370FA57-4757-3604-3648-499E1F642D3F': 'Gekko',
                 'CC8B64C8-4B25-4FF9-6E7F-37B4DA43D235': 'Deadlock',
                 '0E38B510-41A8-5780-5E8F-568B2A4F2D6C': 'Iso',
                 '1DBF2EDD-4729-0984-3115-DAA5EED44993': 'Clove',
                 'EFBA5359-4016-A1E5-7626-B1AE76895940': 'Vyse'}
    
    @staticmethod
    def genGameDataFromJson(path : str):
        # Load data from json file
        raw_data = GameDataCleaner._loadFromJson(path)

        # Create dictionary of rounds
        rounds_dict = GameDataCleaner._createRoundsDict(raw_data)

        # Create dataframe of team performance
        team_pf, round_df = GameDataCleaner._createTeamAndRoundDf(raw_data)

        # Create dataframe of player performance
        player_pf = GameDataCleaner._createPlayerPf(rounds_dict, raw_data)

        #return team performance, round data, and player performance
        return team_pf, round_df, player_pf
    
    @staticmethod
    def _loadFromJson(path : str):
        with open(path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            return pd.DataFrame(raw_data)
    
    @staticmethod
    def _createRoundsDict(raw_data : pd.DataFrame):
        # Filter rows where roundStarted or roundEnded are not NaN
        filtered_df = raw_data[(pd.notna(raw_data['roundStarted'])) | (pd.notna(raw_data['roundEnded']))]

        rounds_dict = {}

        # Loop through the filtered DataFrame
        for index, row in filtered_df.iterrows():
            # Check if the round has started
            if pd.notna(row['roundStarted']):
                round_number = row['roundStarted'].get('roundNumber')
                if round_number:
                    rounds_dict[round_number] = {'start_index': index, 'end_index': None}
    
            # Check if the round has ended
            if pd.notna(row['roundEnded']):
                round_number = row['roundEnded'].get('roundNumber')
                if round_number in rounds_dict:
                    rounds_dict[round_number]['end_index'] = index

        # Remove rounds that don't have both start and end indices
        rounds_dict = {k: v for k, v in rounds_dict.items() if v['end_index'] is not None}

        # Now rounds_dict will contain the start and end index for each round
        return rounds_dict
    
    @staticmethod
    def _createTeamAndRoundDf(raw_data : pd.DataFrame):
        winlossdata = raw_data['gameDecided'].dropna().values[0]

        # Extract the first round's teams
        first_round = winlossdata['spikeMode']['completedRounds'][0]
        team_1_number = first_round['spikeModeResult']['attackingTeam']['value']
        team_2_number = first_round['spikeModeResult']['defendingTeam']['value']

        # Initialize cumulative metrics using Python lists
        team_1_metrics = {'Team': team_1_number, 'Total Wins': 0, 'Attacking Half Wins': 0, 'Defending Half Wins': 0, 'Pistol Round Wins': 0}
        team_2_metrics = {'Team': team_2_number, 'Total Wins': 0, 'Attacking Half Wins': 0, 'Defending Half Wins': 0, 'Pistol Round Wins': 0}

        # Initialize lists to hold the round number and winning team information
        round_numbers = []
        winning_teams = []

        # Loop through the rounds to calculate cumulative metrics and build the round_df
        for round_info in winlossdata['spikeMode']['completedRounds']:
            round_number = round_info['roundNumber']
            winning_team = round_info['winningTeam']['value']
            
            # Add the round number and winning team to their respective lists
            round_numbers.append(round_number)
            winning_teams.append(winning_team)

            # Determine if the round was won while attacking or defending
            attacking_team = round_info['spikeModeResult']['attackingTeam']['value']
            defending_team = round_info['spikeModeResult']['defendingTeam']['value']
            
            if winning_team == attacking_team:
                if winning_team == team_1_number:
                    team_1_metrics['Attacking Half Wins'] += 1
                else:
                    team_2_metrics['Attacking Half Wins'] += 1
            elif winning_team == defending_team:
                if winning_team == team_1_number:
                    team_1_metrics['Defending Half Wins'] += 1
                else:
                    team_2_metrics['Defending Half Wins'] += 1

            # Increment total wins for the winning team
            if winning_team == team_1_number:
                team_1_metrics['Total Wins'] += 1
            else:
                team_2_metrics['Total Wins'] += 1

            # Check if it's a pistol round (round 1 or round 13)
            if round_number == 1 or round_number == 13:
                if winning_team == team_1_number:
                    team_1_metrics['Pistol Round Wins'] += 1
                else:
                    team_2_metrics['Pistol Round Wins'] += 1

        # Combine the round numbers and winning teams into a DataFrame
        round_df = pd.DataFrame({
            'Round Number': round_numbers,
            'Winning Team': winning_teams
        })

        # Set the index to the round number
        round_df.set_index('Round Number', inplace=True)

        # Combine metrics for both teams into a list and convert it into a DataFrame
        all_team_metrics = [team_1_metrics, team_2_metrics]
        team_pf = pd.DataFrame(all_team_metrics)

        # Initialize an empty list for the round ceremony types
        round_ceremony_types = []

        # Extract round ceremonies (assuming the order aligns with round numbers)
        round_ceremony = raw_data['roundCeremony'].dropna()

        # Loop through the available round ceremonies
        for idx, round_info in enumerate(round_df.index):
            # Check if there is a corresponding round ceremony and extract the type
            if idx < len(round_ceremony):
                ceremony = round_ceremony.iloc[idx]
                ceremony_type = ceremony.get('type', 'UNKNOWN')  # Default to 'UNKNOWN' if type is missing
                round_ceremony_types.append(ceremony_type)
            else:
                round_ceremony_types.append(None)  # Append None if no ceremony data is available

        # Add the extracted round ceremony types to round_df
        round_df['Round Ceremony Type'] = round_ceremony_types

        return team_pf, round_df
    
    @staticmethod
    def _createPlayerPf(rounds_dict : dict, raw_data : pd.DataFrame):
        # Initialize a dictionary to store player stats
        player_metrics = {}

        # Iterate over rounds_dict to extract relevant damage events and calculate metrics
        for round_number, indices in rounds_dict.items():
            start_index = indices['start_index']
            end_index = indices['end_index']
            
            # Filter the rows of damage events within the round using vectorized indexing
            round_damage_events = raw_data.loc[start_index:end_index, 'damageEvent'].dropna()

            # Process each damage event and directly update player stats
            for event in round_damage_events:
                causer_id = event['causerId']['value']
                victim_id = event['victimId']['value']
                damage_amount = event['damageAmount']
                kill_event = event['killEvent']
                location = event['location']

                # Initialize or update stats for causer (the one dealing damage)
                if causer_id not in player_metrics:
                    player_metrics[causer_id] = {
                        'kills': 0, 'deaths': 0, 'damage_dealt': 0, 'damage_taken': 0, 'total_hits': 0, 'headshots': 0
                    }

                # Initialize or update stats for victim (the one receiving damage)
                if victim_id not in player_metrics:
                    player_metrics[victim_id] = {
                        'kills': 0, 'deaths': 0, 'damage_dealt': 0, 'damage_taken': 0, 'total_hits': 0, 'headshots': 0
                    }

                # Update causer's stats
                player_metrics[causer_id]['damage_dealt'] += damage_amount
                player_metrics[causer_id]['total_hits'] += 1
                
                if location == 'HEAD':
                    player_metrics[causer_id]['headshots'] += 1

                if kill_event:
                    player_metrics[causer_id]['kills'] += 1

                # Update victim's stats
                player_metrics[victim_id]['damage_taken'] += damage_amount
                if kill_event:
                    player_metrics[victim_id]['deaths'] += 1

        # Calculate headshot percentage for each player
        for player_id, metrics in player_metrics.items():
            total_hits = metrics['total_hits']
            headshots = metrics['headshots']
            metrics['headshot_percentage'] = (headshots / total_hits * 100) if total_hits > 0 else 0

        # Convert the player_metrics dictionary into a DataFrame for analysis
        player_pf = pd.DataFrame.from_dict(player_metrics, orient='index').reset_index()
        player_pf.rename(columns={'index': 'playerID'}, inplace=True)

        final_row = raw_data['snapshot'].iloc[-1]

        # Iterate through playertest['players'] and update player_metrics_df
        for player in final_row['players']:
            player_id = player['playerId']['value']
            
            # Find the index of the player in player_metrics_df
            if player_id in player_pf['playerID'].values:
                # Get the index of the player to update their metrics
                player_index = player_pf[player_pf['playerID'] == player_id].index[0]

                # Update Assists and TotalScore in player_metrics_df
                player_pf.at[player_index, 'Assists'] = player['assists']
                player_pf.at[player_index, 'TotalScore'] = player['scores']['combatScore']['totalScore']

        filtered_df = raw_data[raw_data['configuration'].notna()]
        agent = []

        for player in filtered_df['configuration'].iloc[-1]['players']:
            account_id = player['accountId']['value']
            player_id = player['playerId']['value']
            display_name = player['displayName']
            selected_agent = player['selectedAgent']['fallback']['guid']
            
            agent.append({
                'accountId': account_id,
                'playerId': player_id,
                'displayName': display_name,
                'selectedAgent': selected_agent
            })

        # Create a DataFrame
        agent_df = pd.DataFrame(agent)

        agent_df['AgentName'] = agent_df['selectedAgent'].map(GameDataCleaner.AGENT_MAP)


        player_pf = pd.merge(player_pf, agent_df, left_on='playerID', right_on='playerId')
        player_pf = player_pf.drop(columns = ['selectedAgent','playerId'], axis=1)

        return player_pf
