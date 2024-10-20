import os
import pandas as pd

class PlayerPerformanceAggregator:
    
    def __init__(self, agg_file='player_performance_agg.xlsx'):
        """
        Initialize the PlayerPerformanceAggregator with the file path.
        """
        self.agg_file = agg_file
        self.agg_df = self._load_or_initialize_agg_df()
    
    def _load_or_initialize_agg_df(self):
        """
        Load the existing aggregate DataFrame or initialize a new one if it doesn't exist.
        """
        if os.path.exists(self.agg_file):
            # Load the existing aggregate Excel file if it exists
            agg_df = pd.read_excel(self.agg_file, index_col=0)
            print(f"Loaded existing aggregate data from {self.agg_file}")
        else:
            # Create an empty DataFrame with the relevant columns if the file doesn't exist
            agg_df = pd.DataFrame(columns=['accountId', 'games_played', 'kills', 'deaths', 'damage_dealt',
                                           'damage_taken', 'total_hits', 'headshots', 'Assists', 'TotalScore', 'agent_pool'])
            print(f"Created a new aggregate file at {self.agg_file}")
        return agg_df
    
    def _update_agent_pool(self, existing_pool, new_agent):
        """
        Update the agent pool by adding new agents to the list.
        
        :param existing_pool: The existing agent pool as a comma-separated string.
        :param new_agent: The new agent played by the player in the current game.
        :return: Updated agent pool as a comma-separated string.
        """
        if pd.isna(existing_pool):  # If no agents have been tracked yet
            agent_pool = {new_agent}
        else:
            agent_pool = set(existing_pool.split(','))  # Convert existing pool into a set for uniqueness
            agent_pool.add(new_agent)  # Add the new agent
        
        return ','.join(sorted(agent_pool))  # Return the updated agent pool as a sorted comma-separated string

    def aggregate_player_data(self, player_pf):
        """
        Aggregate the player_pf data into the existing aggregate DataFrame, updating the agent pool.
        
        :param player_pf: DataFrame containing player performance data.
        """
        # Columns to aggregate (skipping accountId, displayName, and AgentName)
        agg_columns = ['kills', 'deaths', 'damage_dealt', 'damage_taken', 
                       'total_hits', 'headshots', 'Assists', 'TotalScore']
        
        avg_columns = [f"avg_{col}" for col in agg_columns]  # Average columns corresponding to the cumulative ones
    
        # Loop through each player in the new player performance data (player_pf)
        for index, row in player_pf.iterrows():
            account_id = row['accountId']
            agent_name = row['AgentName']  # The agent the player used in this game
            
            # Check if the player already exists in the aggregated DataFrame
            if account_id in self.agg_df['accountId'].values:
                # Update the player's cumulative sum and games played
                self.agg_df.loc[self.agg_df['accountId'] == account_id, 'games_played'] += 1
                
                for col in agg_columns:
                    self.agg_df.loc[self.agg_df['accountId'] == account_id, col] += row[col]
                
                # Update the agent pool for the player
                current_pool = self.agg_df.loc[self.agg_df['accountId'] == account_id, 'agent_pool'].values[0]
                updated_pool = self._update_agent_pool(current_pool, agent_name)
                self.agg_df.loc[self.agg_df['accountId'] == account_id, 'agent_pool'] = updated_pool
    
                # Recalculate averages
                games_played = self.agg_df.loc[self.agg_df['accountId'] == account_id, 'games_played'].values[0]
                for col in agg_columns:
                    avg_col = f"avg_{col}"
                    total_value = self.agg_df.loc[self.agg_df['accountId'] == account_id, col].values[0]
                    self.agg_df.loc[self.agg_df['accountId'] == account_id, avg_col] = total_value / games_played
    
            else:
                # Create a new entry for the player (add cumulative values and set average columns to None initially)
                new_row_data = [account_id, 1] + row[agg_columns].tolist() + [agent_name]
                
                # Add None for average columns initially (they'll be calculated after aggregation)
                new_row_data.extend([None] * len(avg_columns))
                
                # Create the new row and append it
                new_row = pd.Series(new_row_data, index=self.agg_df.columns)
                self.agg_df = pd.concat([self.agg_df, pd.DataFrame([new_row])], ignore_index=True)
    
                # Now recalculate the averages for the new player
                for col in agg_columns:
                    avg_col = f"avg_{col}"
                    self.agg_df.loc[self.agg_df['accountId'] == account_id, avg_col] = row[col]  # Initial average is the value itself


    
    def _calculate_averages(self):
        """
        Calculate the averages for performance columns and add them to the DataFrame.
        """
        avg_columns = ['kills', 'deaths', 'damage_dealt', 'damage_taken', 
                       'total_hits', 'headshots', 'Assists', 'TotalScore']
        
        # Calculate average columns by dividing the cumulative values by games_played
        for col in avg_columns:
            avg_col_name = f'avg_{col}'
            self.agg_df[avg_col_name] = self.agg_df[col] / self.agg_df['games_played']
    
    def save_agg_df(self):
        """
        Save the aggregated DataFrame back to the Excel file.
        """
        # Before saving, calculate the averages
        self._calculate_averages()

        try:
            self.agg_df.to_excel(self.agg_file)
            print(f"Aggregated data saved to {self.agg_file}")
        except Exception as e:
            print(f"Error saving data to Excel: {e}")

# Example usage:
# Assuming `player_pf` is the player performance DataFrame you get from GameDataCleaner

def example_usage(player_pf):
    # Create an instance of PlayerPerformanceAggregator
    aggregator = PlayerPerformanceAggregator()
    
    # Aggregate the new player performance data
    aggregator.aggregate_player_data(player_pf)
    
    # Save the updated aggregated data
    aggregator.save_agg_df()

# Example test case (assuming `player_pf` is the DataFrame you are passing):
# Call `example_usage(player_pf)` after you get `player_pf` from GameDataCleaner
