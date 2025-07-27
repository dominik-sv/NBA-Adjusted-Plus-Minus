from get_lineups import get_lineups
from nba_api.stats.endpoints import PlayByPlayV3
from nba_api.stats.endpoints import boxscoretraditionalv2
import pandas as pd
import numpy as np
import re
import json
from tqdm import tqdm
import os

with open('game_ids.json', 'r') as f:
    game_ids = json.load(f)

for key, game_id in tqdm(list(game_ids.items())[16:30]):

    TIP_TO_RE = re.compile(r"Tip to ([A-Za-z .'-]+)")
    PERIOD_RE = re.compile(r"(Start|End) of (\d)(st|nd|rd|th) (Period|OT).*")

    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).get_data_frames()[0]
    if boxscore.empty:
        print('DataFrame is empty')
    boxscore['LAST_NAME'] = boxscore.apply(lambda row: row['PLAYER_NAME'].replace(row['NICKNAME'], '').strip(), axis = 1)
    player_team_dict = {}
    teams = []
    for idx, player in boxscore.iterrows():
        if player['MIN'] == None:
            continue

        last_name = player['LAST_NAME']
        team = player['TEAM_ABBREVIATION']
        player_team_dict[last_name] = team

        if team not in teams:
            teams.append(team)

    # def other_team(team, teams = teams):
    #     return teams[0] if team == teams[1] else teams[1]

    # pbp = PlayByPlayV3(game_id=game_id).get_data_frames()[0]

    # team_tricode_dictionary = {}
    # team_tricodes = pbp['teamTricode'].unique()[1:]
    # team_tricode_dictionary[team_tricodes[0]] = pbp[pbp['teamTricode'] == team_tricodes[0]]['location'].unique()[0]
    # team_tricode_dictionary[team_tricodes[1]] = pbp[pbp['teamTricode'] == team_tricodes[1]]['location'].unique()[0]
    # team_tricode_dictionary

    # pbp['time_in_period'] = (12 * 60 - (60 * pbp['clock'].str[2:4].astype(int) + pbp['clock'].str[5:10].astype(float))) * 10
    # pbp['period'] = pbp['period'].astype(int)
    # regular_periods = (pbp['period'].clip(upper=4) - 1)
    # ot_periods = (pbp['period'] - 5).clip(lower=0)
    # pbp['time'] = regular_periods * 60 * 12 * 10 + ot_periods * 60 * 5 * 10 + pbp['time_in_period']
    # pbp[['scoreHome', 'scoreAway']] = pbp[['scoreHome', 'scoreAway']].replace('', np.nan)
    # pbp[['scoreHome', 'scoreAway']] = pbp[['scoreHome', 'scoreAway']].ffill()

    # plays = pbp[['actionNumber', 'teamId', 'scoreHome', 'scoreAway', 'description', 'actionType', 'subType', 'time', 'location']].copy()
    # plays['newPossession'] = False
    # plays['possession'] = None

    # for idx, row in plays.iterrows():
    #     actionType = row['actionType']
    #     subType = row['subType']
    #     actionNumber = row['actionNumber']
    #     location = row['location']
    #     description = row['description']

    #     if actionType == 'Made Shot':

    #         next_idx = idx + 1
    #         while plays.at[next_idx, 'actionNumber'] == actionNumber or (plays.at[next_idx, 'actionType'] in ('Substitution', 'Timeout')):
    #             next_idx += 1
    #         next_play = plays.loc[next_idx]

    #         # And 1 play
    #         if (next_play['actionType'] == 'Foul') and (next_play['subType'] == 'Shooting') and (next_play['location'] != location): # in ('Shooting', 'Flagrant')
    #             continue

    #         # Regular basket made
    #         plays.at[idx + 1, 'newPossession'] = True

    #     elif actionType == 'Turnover':
    #         plays.at[idx + 1, 'newPossession'] = True

    #     elif actionType == 'Missed Shot':

    #         next_idx = idx + 1
    #         while plays.at[next_idx, 'actionNumber'] == actionNumber:
    #             next_idx += 1
    #         next_play = plays.loc[next_idx]

    #         assert next_play['actionType'] == 'Rebound', f"Next play {next_play['description']} (action {next_play['actionNumber']}) (row {idx}) after {row['description']} is not rebound"
    #         if location != next_play['location']:
    #             plays.at[next_idx, 'newPossession'] = True

    #     elif actionType == 'Foul' and subType == 'Transition Take':
    #         retain_possession_free_throw = True

    #     elif actionType == 'Free Throw' and subType not in ('Free Throw 1 of 2', 'Free Throw 1 of 3', 'Free Throw 2 of 3', 'Free Throw Technical'):

    #         next_idx = idx + 1
    #         while (plays.at[next_idx, 'actionNumber'] == actionNumber) or (plays.at[next_idx, 'actionType'] in ('Substitution', 'Timeout')):
    #             next_idx += 1
    #         next_play = plays.loc[next_idx]

    #         if next_play['actionType'] in ('Foul', 'Violation'):
    #             if location == next_play['location']:
    #                 plays.at[next_idx, 'newPossession'] = True
    #         elif next_play['actionType'] == 'period':
    #             continue
    #         else:
    #             if location != next_play['location']:
    #                 plays.at[next_idx, 'newPossession'] = True

    #     elif actionType == 'Jump Ball':
    #         last_idx = idx - 1
    #         while (plays.at[last_idx, 'actionNumber'] == actionNumber) or (plays.at[last_idx, 'actionType'] in ('Substitution', 'Timeout', 'Foul', 'Violation')):
    #             last_idx -= 1
    #         last_play = plays.loc[last_idx]

    #         # Jump ball at new period
    #         if last_play['actionType'] == 'period':

    #             # Who got the ball
    #             m = TIP_TO_RE.search(description)
    #             assert m, f"No player found in description: {description} (row {idx})"
    #             player = m.group(1).strip()
    #             assert player in player_team_dict, f"{player} not found in player_team_dict"
    #             team = player_team_dict[player]

    #             plays.at[idx, 'newPossession'] = True
    #             plays.at[idx, 'possession'] = team

    #             # Save tipoff winner
    #             if idx == 1:
    #                 initial_tip = team

    #         # In game jump ball
    #         else:

    #             # Who got the ball
    #             m = TIP_TO_RE.search(description)
    #             assert m, f"No player found in description: {description} (row {idx})"
    #             player = m.group(1).strip()
    #             assert player in player_team_dict, f"{player} not found in player_team_dict"
    #             team = player_team_dict[player]
    #             team_home_or_visitor = team_tricode_dictionary[team]  # team has to be converted to v and h

    #             # Who had possession of the ball in previous play
    #             if last_play['actionType'] == 'Rebound':
    #                 last_possession = last_play['location']
    #             else:
    #                 last_possession = other_team(last_play['location'], teams = ['h', 'v'])

    #             # Evaluate wether it is a new possession
    #             if team_home_or_visitor != last_possession:  
    #                 plays.at[idx, 'newPossession'] = True

    #     elif actionType == 'period':
    #         if subType == 'start':
    #             m = PERIOD_RE.search(description)
    #             assert m, f"No period found in description: {description}"
    #             period = int(m.group(2))
    #             quarter_type = m.group(4)

    #             if period in (2, 3, 4) and quarter_type == 'Period':
    #                 plays.at[idx, 'newPossession'] = True

    #         # Rewrites any newPossession = True that could be carried over from previous plays
    #             elif quarter_type == 'OT':
    #                     plays.at[idx, 'newPossession'] = False
                        
    #         elif subType == 'end':
    #             plays.at[idx, 'newPossession'] = False


    # for idx, row in plays.iterrows():
    #     possession_end = row['newPossession']
    #     actionType = row['actionType']
    #     subType = row['subType']
    #     description = row['description']

    #     if actionType == 'period' and subType == 'start':
    #         m = PERIOD_RE.search(description)
    #         assert m, f"No period found in description: {description}"
    #         period = int(m.group(2))
    #         quarter_type = m.group(4)

    #         if quarter_type == 'Period':
    #             if period in (2, 3):
    #                 plays.at[idx, 'possession'] = other_team(initial_tip)

    #             if period == 4:
    #                 plays.at[idx, 'possession'] = initial_tip        

    #     elif actionType == 'Jump Ball' and plays.at[idx - 1, 'actionType'] == 'period':
    #         m = TIP_TO_RE.search(description)
    #         assert m, f"No player found in description: {description} (row {idx})"
    #         player = m.group(1).strip()
    #         assert player in player_team_dict, f"{player} not found in player_team_dict"
    #         team = player_team_dict[player]
    #         plays.at[idx, 'possession'] = team

    #     elif idx > 1:
    #         last_possession = plays.at[idx - 1, 'possession']
    #         if possession_end:
    #             plays.at[idx, 'possession'] = other_team(last_possession)
    #         else:
    #             plays.at[idx, 'possession'] = last_possession


    # plays['possessionCount'] = plays['newPossession'].cumsum()

    # directory = 'plays'
    # path = os.path.join(directory, f'plays_{key}.csv')
    # plays.to_csv(path)

    directory2 = 'player_team_dict'
    path2 = os.path.join(directory2, f'dict_{key}.json')
    with open(path2, "w") as f:
        json.dump(game_ids, f)