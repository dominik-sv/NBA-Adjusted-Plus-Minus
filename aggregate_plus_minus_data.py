from nba_api.stats.endpoints import LeagueGameLog
import pandas as pd
import numpy as np
from get_lineups import get_lineups
from label_play_by_play import get_labelled_play_by_play
from tqdm import tqdm

seasons = ['2024-25']

# Get game_ids
for season in seasons:
    game_log = LeagueGameLog(season=season).get_data_frames()[0]

df = pd.DataFrame(game_log)
game_ids = df['GAME_ID'].unique()

def calculate_diff(series: pd.Series) -> pd.Series:
    """
    Calculate the difference between consecutive elements in a pandas Series.
    The first element is set to its original value.
    Parameters
    ----------
    series : pd.Series
        The input pandas Series for which to calculate the difference.
    Returns
    -------
    pd.Series
        A new pandas Series containing the differences between consecutive elements.
    """

    s = series.astype(int)
    diff = s.diff()
    diff.iat[0] = s.iat[0]
    return diff.astype(int)

# Create final output Dataframe
columns = ['ID'] + [f'P{i}{loc}' for loc in ['H', 'V'] for i in range(1, 6)] + ['Plus_Off', 'Minus_Def', 'Plus/Minus'] + ['Home_Poss_Off', 'Home_Poss_Def', 'Poss_Tot'] + ['Time']
df = pd.DataFrame(columns=columns)
columns_to_keep = ['ID'] + [f'Player_{i}_{loc}_ID' for loc in ['Home', 'Away'] for i in range(1, 6)]

problem = 0
# Get data
for game_id in tqdm(game_ids[:5]):
    try:
        lineup = get_lineups(game_id)
        pbp = get_labelled_play_by_play(game_id)
        pbp = pbp.sort_values(by='time')
        lineup = lineup.sort_values(by='End_Time')

        # Transform data
        merged = pd.merge_asof(lineup, pbp, left_on = 'End_Time', right_on= 'time', direction = 'backward')

        merged['scoreHome'] = calculate_diff(merged['scoreHome'])
        merged['scoreAway'] = calculate_diff(merged['scoreAway'])
        merged['plusMinus'] = merged['scoreHome'] - merged['scoreAway']

        merged['possessionH'] = calculate_diff(merged['possessionH'])
        merged['possessionV'] = calculate_diff(merged['possessionV'])
        merged['possessionCount'] = calculate_diff(merged['possessionCount'])

        # Create output
        for idx, row in merged.iterrows():
            id = row['ID']

            time = row['End_Time'] - row['Start_Time'] 

            if len(df[df['ID'] == id]) == 0:
                initializing_row = [row[col] for col in columns_to_keep] + [0, 0, 0, 0, 0, 0, 0]
                init_df_row = pd.DataFrame([initializing_row], columns=df.columns)
                if df.empty:
                    df = init_df_row
                else:
                    df = pd.concat([df, init_df_row], ignore_index=True)

            assert len(df[df['ID'] == id]) == 1, f"Multiple rows with same lineup ID"
            mask = df['ID'] == id
            df.loc[mask, 'Plus_Off'] += row['scoreHome']
            df.loc[mask, 'Minus_Def'] += row['scoreAway']
            df.loc[mask, 'Plus/Minus'] += row['plusMinus']
            df.loc[mask, 'Home_Poss_Off'] += row['possessionH']
            df.loc[mask, 'Home_Poss_Def'] += row['possessionV']
            df.loc[mask, 'Poss_Tot'] += row['possessionCount']
            df.loc[mask, 'Time'] += time

    except Exception as e:
        problem += 1
        print(f"Error processing game ID {game_id}: {e}")
        continue

df['Off_Rating'] = df['Plus_Off'] / df['Home_Poss_Off'] * 100
df.loc[df['Home_Poss_Off'] == 0, 'Off_Rating'] = np.nan
df['Def_Rating'] = df['Minus_Def'] / df['Home_Poss_Def'] * 100
df.loc[df['Home_Poss_Def'] == 0, 'Def_Rating'] = np.nan
df['Net_Rating'] = df['Off_Rating'] - df['Def_Rating']


# Export to CSV
df.to_csv('lineup_plus_minus.csv', index=False)
print(f"Problems encountered: {problem}")