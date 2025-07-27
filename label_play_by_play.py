from get_lineups import get_lineups
from nba_api.stats.endpoints import PlayByPlayV3
from nba_api.stats.endpoints import boxscoretraditionalv2
import pandas as pd
import numpy as np
import re
import json
from tqdm import tqdm
import os


def get_team_player_dict(game_id: str) -> tuple[dict, list]:

    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).get_data_frames()[0]
    if boxscore.empty:
        print('DataFrame is empty')
    boxscore['LAST_NAME'] = boxscore.apply(lambda row: row['PLAYER_NAME'].replace(row['NICKNAME'], '').strip(), axis = 1)
    team_player_dict = {}
    teams = []
    for idx, player in boxscore.iterrows():
        if player['MIN'] == None:
            continue

        last_name = player['LAST_NAME']
        team = player['TEAM_ABBREVIATION']
        team_player_dict[last_name] = team

        if team not in teams:
            teams.append(team)

    return team_player_dict, teams


def find_next_play(idx: int, actionNumber: int, plays: pd.DataFrame, reverse: bool = False) -> tuple[pd.Series, int]:
    increment = 1
    if reverse:
        increment = -1

    next_idx = idx + increment
    while plays.at[next_idx, 'actionNumber'] == actionNumber or plays.at[next_idx, 'actionType'] in ('Substitution', 'Timeout') or (plays.at[next_idx, 'actionType'] == 'Foul' and plays.at[next_idx, 'subType'] == 'Double Personal'):
        next_idx += increment
    next_play = plays.loc[next_idx]

    return next_play, next_idx


def determine_play_possession(play: pd.Series) -> str:
    actionType = play['actionType']
    location = play['location']

    if actionType in ('Foul', 'Violation'):
        team_poss = get_other_team(location, ['h', 'v'])
    else:
        team_poss = location

    return team_poss


def get_other_team(team: str, teams: list) -> str:
    assert team in teams, f"Parameter team ({team}) must be present in parameter teams ({teams})"
    return teams[0] if team == teams[1] else teams[1]


def get_labelled_play_by_play(game_id: str) -> pd.DataFrame:

    TIP_TO_RE = re.compile(r"Tip to ([A-Za-z .'-]+)")
    PERIOD_RE = re.compile(r"(Start|End) of (\d)(st|nd|rd|th) (Period|OT).*")

    team_player_dict, teams = get_team_player_dict(game_id=game_id)

    # Call Play by Play
    pbp = PlayByPlayV3(game_id=game_id).get_data_frames()[0]

    # Get team tricode dictionary (ttd[MIA] = 'v')
    team_tricode_dictionary = {}
    team_tricodes = pbp['teamTricode'].unique()[1:]
    team_tricode_dictionary[team_tricodes[0]] = pbp[pbp['teamTricode'] == team_tricodes[0]]['location'].unique()[0]
    team_tricode_dictionary[team_tricodes[1]] = pbp[pbp['teamTricode'] == team_tricodes[1]]['location'].unique()[0]

    # Add/edit columns in pbp
    pbp['time_in_period'] = (12 * 60 - (60 * pbp['clock'].str[2:4].astype(int) + pbp['clock'].str[5:10].astype(float))) * 10
    pbp['period'] = pbp['period'].astype(int)
    regular_periods = (pbp['period'].clip(upper=4) - 1)
    ot_periods = (pbp['period'] - 5).clip(lower=0)
    pbp['time'] = regular_periods * 60 * 12 * 10 + ot_periods * 60 * 5 * 10 + pbp['time_in_period']
    pbp[['scoreHome', 'scoreAway']] = pbp[['scoreHome', 'scoreAway']].replace('', np.nan)
    pbp[['scoreHome', 'scoreAway']] = pbp[['scoreHome', 'scoreAway']].ffill()

    # Create new dataframe with needed columns
    plays = pbp[['actionNumber', 'teamId', 'scoreHome', 'scoreAway', 'description', 'actionType', 'subType', 'time', 'location']].copy()
    plays['newPossession'] = False
    plays['possession'] = None


    # Determine new possessions
    for idx, row in plays.iterrows():
        actionType = row['actionType']
        subType = row['subType']
        actionNumber = row['actionNumber']
        location = row['location']
        description = row['description']

        # Made Shot
        if actionType == 'Made Shot':

            next_play, next_idx = find_next_play(idx, actionNumber, plays)

            # And 1 play
            if (next_play['actionType'] == 'Foul') and (next_play['subType'] == 'Shooting') and (next_play['location'] != location):
                continue

            # Regular basket made
            plays.at[idx + 1, 'newPossession'] = True

        # Turnover
        elif actionType == 'Turnover':
            plays.at[idx + 1, 'newPossession'] = True

        # Missed Shot
        elif actionType == 'Missed Shot':

            next_play, next_idx = find_next_play(idx, actionNumber, plays)

            assert next_play['actionType'] == 'Rebound', f"Next play {next_play['actionType']} is not a rebound (row {idx})"
            
            if location != next_play['location']:
                plays.at[next_idx, 'newPossession'] = True

        # Free Throw
        elif actionType == 'Free Throw':

            next_play, next_idx = find_next_play(idx, actionNumber, plays)

            if next_play['actionType'] == 'period':
                continue

            next_play_team_poss = determine_play_possession(next_play)
            
            if location != next_play_team_poss:
                plays.at[next_idx, 'newPossession'] = True

        # Jump Ball
        elif actionType == 'Jump Ball':

            last_play, last_idx = find_next_play(idx, actionNumber, plays, reverse = True)

            # Jump ball at new period
            if last_play['actionType'] == 'period':

                # Who got the ball (NEW based on next play)
                next_play, next_idx = find_next_play(idx, actionNumber, plays)
                next_play_team_poss = determine_play_possession(next_play)
                if team_tricode_dictionary.values()[0] == next_play_team_poss:
                    team = team_tricode_dictionary.keys()[0]
                else:
                    team = team_tricode_dictionary.keys()[1]

                # # Who got the ball
                # m = TIP_TO_RE.search(description)
                # assert m, f"No player found in description: {description} (row {idx})"
                # player = m.group(1).strip()
                # assert player in team_player_dict, f"{player} not found in team_player_dict"
                # team = team_player_dict[player]

                plays.at[idx, 'newPossession'] = True
                plays.at[idx, 'possession'] = team

                # Save tipoff winner
                if idx == 1:
                    initial_tip = team

            # # In game jump ball
            # else:

            #     # Who got the ball (NEW based on next play)
            #     next_play, next_idx = find_next_play(idx, actionNumber, plays)
            #     next_play_team_poss = determine_play_possession(next_play)
            #     if team_tricode_dictionary.values()[0] == next_play_team_poss:
            #         team = team_tricode_dictionary.keys()[0]
            #     else:
            #         team = team_tricode_dictionary.keys()[1]



                # ## IS JUMP BALL SWITCH POSSESSIONS A TURNOVER??

                # # Who got the ball
                # m = TIP_TO_RE.search(description)
                # if not m:
                #     continue
                # assert m, f"No player found in description: {description} (row {idx})"
                # player = m.group(1).strip()
                # assert player in team_player_dict, f"{player} not found in team_player_dict"
                # team = team_player_dict[player]
                # team_home_or_visitor = team_tricode_dictionary[team]  # team has to be converted to v and h

                # # Who had possession of the ball in previous play
                # if last_play['actionType'] == 'Rebound':
                #     last_possession = last_play['location']
                # else:
                #     last_possession = get_other_team(last_play['location'], teams = ['h', 'v'])

                # # Evaluate wether it is a new possession
                # if team_home_or_visitor != last_possession:  
                #     plays.at[idx, 'newPossession'] = True

        # Period
        elif actionType == 'period':
            if subType == 'start':
                m = PERIOD_RE.search(description)
                assert m, f"No period found in description: {description}"
                period = int(m.group(2))
                quarter_type = m.group(4)

                if quarter_type == 'Period':
                    if period in (2, 3):
                        plays.at[idx, 'newPossession'] = True
                        plays.at[idx, 'possession'] = get_other_team(initial_tip, teams)

                    if period == 4:
                        plays.at[idx, 'newPossession'] = True
                        plays.at[idx, 'possession'] = initial_tip 

                # Rewrites any newPossession = True that could be carried over from previous plays
                elif quarter_type == 'OT':
                        plays.at[idx, 'newPossession'] = False


    # Label possessions by team
    for idx, row in plays.iterrows():
        possession_end = row['newPossession']
        actionType = row['actionType']
        subType = row['subType']
        description = row['description']
        possession = row['possession']

        if idx == 0:
            continue
        # if actionType == 'period' and subType == 'start':
        #     m = PERIOD_RE.search(description)
        #     assert m, f"No period found in description: {description}"
        #     period = int(m.group(2))
        #     quarter_type = m.group(4)

        #     if quarter_type == 'Period':
        #         if period in (2, 3):
        #             plays.at[idx, 'possession'] = get_other_team(initial_tip, teams)

        #         if period == 4:
        #             plays.at[idx, 'possession'] = initial_tip        

        # elif actionType == 'Jump Ball' and plays.at[idx - 1, 'actionType'] == 'period':
        #     m = TIP_TO_RE.search(description)
        #     assert m, f"No player found in description: {description} (row {idx})"
        #     player = m.group(1).strip()
        #     assert player in team_player_dict, f"{player} not found in team_player_dict"
        #     team = team_player_dict[player]
        #     plays.at[idx, 'possession'] = team

        if possession is None:
            last_possession = plays.at[idx - 1, 'possession']
            if possession_end:
                plays.at[idx, 'possession'] = get_other_team(last_possession, teams)
            else:
                plays.at[idx, 'possession'] = last_possession


    # Label possession count
    plays['possessionCount'] = plays['newPossession'].cumsum()

    # Export
    directory = 'plays'
    path = os.path.join(directory, f'plays_{key}.csv')
    plays.to_csv(path)

    directory2 = 'team_player_dict'
    os.makedirs(directory2, exist_ok=True)
    path2 = os.path.join(directory2, f'dict_{key}.json')
    with open(path2, "w") as f:
        json.dump(team_player_dict, f)

    return plays


with open('game_ids.json', 'r') as f:
    game_ids = json.load(f)

for key, game_id in tqdm(list(game_ids.items())[:30]):
    get_labelled_play_by_play(game_id=game_id)
