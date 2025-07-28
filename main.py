from nba_api.stats.endpoints import LeagueGameLog
import pandas as pd
import numpy as np
from get_lineups import get_lineups
from label_play_by_play import get_labelled_play_by_play

seasons = ['2024-25']

# Get game_ids
for season in seasons:
    game_log = LeagueGameLog(season=season).get_data_frames()[0]

df = pd.DataFrame(game_log)
game_ids = df['GAME_ID'].unique()

for game_id in game_ids:
    lineup = get_lineups(game_id)
    pbp = get_labelled_play_by_play(game_id)
    pd.merge()