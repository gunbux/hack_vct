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
                                           'damage_taken', 'total_hits', 'headshots', 'Assists', 'TotalScore'])
            print(f"Created a new aggregate file at {self.agg_file}")
        return agg_df
    
    def aggregate_player_data(self, player_pf):
        """
        Aggregate the player_pf data into the existing aggregate DataFrame.
        
        :param player_pf: DataFrame containing player performance data.
        """
        # Columns to aggregate (skipping accountId, displayName, and AgentName)
        agg_columns = ['kills', 'deaths', 'damage_dealt', 'damage_taken', 
                       'total_hits', 'headshots', 'Assists', 'TotalScore']

        # Loop through each player in the new player performance data (player_pf)
        for index, row in player_pf.iterrows():
            account_id = row['accountId']
            
            # Check if the player already exists in the aggregated DataFrame
            if account_id in self.agg_df['accountId'].values:
                # Update the player's cumulative sum and games played
                self.agg_df.loc[self.agg_df['accountId'] == account_id, 'games_played'] += 1
                for col in agg_columns:
                    self.agg_df.loc[self.agg_df['accountId'] == account_id, col] += row[col]
            else:
                # Add a new entry for the player (only aggregate relevant columns)
                new_row = pd.Series([account_id, 1] + row[agg_columns].tolist(), index=self.agg_df.columns)
                self.agg_df = self.agg_df.append(new_row, ignore_index=True)
    
    def save_agg_df(self):
        """
        Save the aggregated DataFrame back to the Excel file.
        """
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
