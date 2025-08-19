import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from requests.exceptions import ReadTimeout
import time


def safe_retry(func, *args, retries=3, backoff=5, backoff_factor=2, **kwargs):
    """
    Call `func(*args, **kwargs)`, retrying up to `retries` times on ReadTimeout.
    Sleeps `backoff` seconds before first retry, then multiplies by backoff_factor.
    """
    delay = backoff
    for attempt in range(1, retries+1):
        try:
            return func(*args, **kwargs)
        except (ReadTimeout, ConnectionError) as e:
            if attempt == retries:
                raise
            print(f"{type(e).__name__} on attempt {attempt}/{retries}, retrying in {delay:.0f}s…")
            time.sleep(delay)
            delay *= backoff_factor

session = requests.Session()

# Wrap session.request to inject default timeout
_orig_request = session.request
def _request_with_timeout(method, url, **kwargs):
    kwargs.setdefault("timeout", (5, 60))
    return _orig_request(method, url, **kwargs)
session.request = _request_with_timeout

# Configure retry strategy
retry_strategy = Retry(
    total=5,                       # up to 5 attempts
    backoff_factor=1,              # 1s → 2s → 4s → 8s → 16s
    status_forcelist=[429,500,502,503,504],
    allowed_methods=False
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# 2) Monkey-patch NBA API’s HTTP layer *before* importing any endpoints:
import nba_api.stats.library.http as http_lib
http_lib.Session = lambda: session

from nba_api.stats.endpoints import LeagueGameLog
import pandas as pd
import numpy as np
from get_lineups import get_lineups
from label_play_by_play import get_labelled_play_by_play
from tqdm import tqdm
import os

part = 11


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

seasons = ['2024-25']
directory = 'data'

# Create final output Dataframe
columns = ['ID'] + [f'P{i}{loc}' for loc in ['H', 'V'] for i in range(1, 6)] + ['Plus_Off', 'Minus_Def', 'Plus/Minus'] + ['Home_Poss_Off', 'Home_Poss_Def', 'Poss_Tot'] + ['Time']
df = pd.DataFrame(columns=columns)
columns_to_keep = ['ID'] + [f'Player_{i}_{loc}_ID' for loc in ['Home', 'Away'] for i in range(1, 6)]

problem = 0
problematic_lineup = 0
empty_boxscore_df = 0
nobody_played = 0

# Get game_ids
for season in seasons:
    game_log = LeagueGameLog(season=season).get_data_frames()[0]

    game_log = pd.DataFrame(game_log)
    game_ids = game_log['GAME_ID'].unique()

    # Get data
    i = 1100
    for game_id in tqdm(game_ids[1100:], desc= f"Processing season {season}"):
        i += 1
        time.sleep(2)

        try:
            lineup = safe_retry(get_lineups, game_id=game_id, retries=3, backoff=60*20)
            if lineup is None:
                problematic_lineup += 1
                continue
        except Exception as e:
            problem += 1
            print(f"[Lineups] {game_id} → {e}")
            continue

        time.sleep(2)
        try:
            pbp = safe_retry(get_labelled_play_by_play, game_id=game_id, retries=3, backoff=60*20)
            if pbp is None:
                empty_boxscore_df += 1
                continue
            if isinstance(pbp, str) and pbp == 'Nobody played':
                nobody_played += 1
                continue
        except Exception as e:
            problem += 1
            print(f"[PBP] {game_id} → {e}")
            continue

        try:
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

                time_played = row['End_Time'] - row['Start_Time'] 

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
                df.loc[mask, 'Time'] += time_played
 
        except Exception as e:
            problem += 1
            print(f"Error processing game ID {game_id}: {e}")
            continue

        if i % 100 == 0:
            part = i // 100

            # Final additions
            df['Off_Rating'] = df['Plus_Off'] / df['Home_Poss_Off'] * 100
            df.loc[df['Home_Poss_Off'] == 0, 'Off_Rating'] = np.nan
            df['Def_Rating'] = df['Minus_Def'] / df['Home_Poss_Def'] * 100
            df.loc[df['Home_Poss_Def'] == 0, 'Def_Rating'] = np.nan
            df['Net_Rating'] = df['Off_Rating'] - df['Def_Rating']
            df['h'] = (df['Home_Poss_Def'] + df['Home_Poss_Off']) / (df['Home_Poss_Def'] * df['Home_Poss_Off'])
            df['Season'] = season

            # Export
            os.makedirs(directory, exist_ok=True)
            filepath = os.path.join(directory, f'data_{season}_{part}.csv')
            df.to_csv(filepath, index=False)

            # Setup
            df = pd.DataFrame(columns=columns)
            time.sleep(10)

    # Final additions
    df['Off_Rating'] = df['Plus_Off'] / df['Home_Poss_Off'] * 100
    df.loc[df['Home_Poss_Off'] == 0, 'Off_Rating'] = np.nan
    df['Def_Rating'] = df['Minus_Def'] / df['Home_Poss_Def'] * 100
    df.loc[df['Home_Poss_Def'] == 0, 'Def_Rating'] = np.nan
    df['Net_Rating'] = df['Off_Rating'] - df['Def_Rating']
    df['h'] = (df['Home_Poss_Def'] + df['Home_Poss_Off']) / (df['Home_Poss_Def'] * df['Home_Poss_Off'])
    df['Season'] = season

    # Problems
    print(f"Number of problems encountered: {problem}")
    print(f"Problematic lineups (API issue): {problematic_lineup}")
    print(f"Empty boxscore dataframes: {empty_boxscore_df}")
    print(f"Nobody played: {nobody_played}")
    
    # Export
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, f'data_{season}_{part+1}.csv')
    df.to_csv(filepath, index=False)

    # Setup
    df = pd.DataFrame(columns=columns)
    time.sleep(10)
