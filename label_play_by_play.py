from get_lineups import get_lineups
from nba_api.stats.endpoints import PlayByPlayV3
from nba_api.stats.endpoints import boxscoretraditionalv2
import pandas as pd
import numpy as np
import re
import json
from tqdm import tqdm
import os
import time


def get_team_player_dict(game_id: str) -> tuple[dict, list]:
    """
    Builds a mapping of player last names to their team abbreviations and returns the list of teams in the game.

    Parameters
    ----------
    game_id : str
        The unique identifier for the NBA game.

    Returns
    -------
    team_player_dict : dict
        Keys are player last names (str), values are team abbreviations (str).
    teams : list of str
        List of unique team abbreviations participating in the game.
    """

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
    """
    Finds the next valid play in a play-by-play DataFrame, skipping substitutions, timeouts, duplicate action numbers, and certain fouls.

    Parameters
    ----------
    idx : int
        Index of the current row in `plays`.
    actionNumber : int
        The action number of the current play, used to skip over duplicates.
    plays : pandas.DataFrame
        DataFrame of play-by-play events containing at least 'actionNumber', 'actionType', and 'subType'.
    reverse : bool, optional
        If True, search backward; otherwise, search forward. Default is False.

    Returns
    -------
    next_play : pandas.Series
        The row corresponding to the next valid play.
    next_idx : int
        The DataFrame index of the next valid play.
    """

    increment = 1
    if reverse:
        increment = -1

    next_idx = idx + increment
    while plays.at[next_idx, 'actionNumber'] == actionNumber or plays.at[next_idx, 'actionType'] in ('Substitution', 'Timeout') or (plays.at[next_idx, 'actionType'] == 'Foul' and plays.at[next_idx, 'subType'] in ('Double Personal', 'Technical', 'Too Many Players Technical', 'Double Technical')):
        next_idx += increment
    next_play = plays.loc[next_idx]

    return next_play, next_idx


def determine_play_possession(play: pd.Series) -> str:
    """
    Determines which team (home or visitor) has possession in a given play.

    Parameters
    ----------
    play : pandas.Series
        A row from the play-by-play DataFrame containing:

    Returns
    -------
    team_possession : str
        The location code ('h' or 'v') indicating which team has possession after the play.
    """

    actionType = play['actionType']
    subType = play['subType']
    location = play['location']

    match actionType:
        case 'Foul':
            if subType in ('Offensive Charge', 'Offensive'):
                team_poss = location
            else:
                team_poss = get_other_value(location, ['h', 'v'])  
        case 'Violation':
            team_poss = get_other_value(location, ['h', 'v'])  
        case _:
            team_poss = location

    return team_poss


def get_other_value(value: str, list: list) -> str:
    """
    Returns the other value from a two-element list given one of the values.

    Parameters
    ----------
    value : str
        One of the two values in `values_list`.
    values_list : list of str
        A list containing exactly two values.

    Returns
    -------
    other_value : str
        The other element from `values_list`.

    Raises
    ------
    AssertionError
        If `value` is not present in `values_list`.
        If `values_list` does not contain exactly two elements.
    """

    assert value in list, f"Value ({value}) must be present in list ({list})"
    assert len(list) == 2, f"List must contain exactly two values (contains {len(list)} values)"
    return list[0] if value == list[1] else list[1]


def get_labelled_play_by_play(game_id: str) -> pd.DataFrame:
    """
    Fetches play-by-play data for a game, annotates new possessions, and labels each play with possession information.

    Parameters
    ----------
    game_id : str
        The unique identifier for the NBA game.

    Returns
    -------
    plays : pandas.DataFrame
        Original play-by-play data with additional columns:
        - newPossession  : bool
            True if the play starts a new possession.
        - possession     : str
            Team tricode of the team in possession.
        - possessionCount: int
            Cumulative count of possessions up to each play.
    """



    PERIOD_RE = re.compile(r"(Start|End) of (\d)(st|nd|rd|th) (Period|OT).*")

    team_player_dict, teams = get_team_player_dict(game_id=game_id)

    # Call Play by Play
    pbp = PlayByPlayV3(game_id=game_id).get_data_frames()[0]

    # Get team tricode dictionary and team_location_dictionary
    team_tricodes = pbp['teamTricode'].unique()[1:]
    loc_0 = pbp[pbp['teamTricode'] == team_tricodes[0]]['location'].unique()[0]
    loc_1 = pbp[pbp['teamTricode'] == team_tricodes[1]]['location'].unique()[0]

    team_tricode_dictionary = {}
    team_tricode_dictionary[team_tricodes[0]] = loc_0
    team_tricode_dictionary[team_tricodes[1]] = loc_1

    team_location_dictionary = {}
    team_location_dictionary[loc_0] = team_tricodes[0]
    team_location_dictionary[loc_1] = team_tricodes[1]

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
        match actionType:
            case 'Made Shot':

                next_play, next_idx = find_next_play(idx, actionNumber, plays)

                # And 1 play
                if (next_play['actionType'] == 'Foul') and (next_play['subType'] == 'Shooting') and (next_play['location'] != location):
                    continue

                # Regular basket made
                plays.at[idx + 1, 'newPossession'] = True

            # Turnover
            case 'Turnover':
                plays.at[idx + 1, 'newPossession'] = True

            # Missed Shot
            case 'Missed Shot':

                next_play, next_idx = find_next_play(idx, actionNumber, plays)

                assert next_play['actionType'] == 'Rebound', f"Next play {next_play['actionType']} is not a rebound (row {idx})"
                
                if location != next_play['location']:
                    plays.at[next_idx, 'newPossession'] = True

            # Free Throw
            case 'Free Throw':

                next_play, next_idx = find_next_play(idx, actionNumber, plays)

                if next_play['actionType'] == 'period':
                    continue

                next_play_team_poss = determine_play_possession(next_play)
                
                if location != next_play_team_poss:
                    plays.at[next_idx, 'newPossession'] = True

            # Jump Ball
            case 'Jump Ball':

                last_play, last_idx = find_next_play(idx, actionNumber, plays, reverse = True)

                # Jump ball at new period
                if last_play['actionType'] == 'period':

                    # Who got the ball (NEW based on next play)
                    next_play, next_idx = find_next_play(idx, actionNumber, plays)
                    next_play_team_poss = determine_play_possession(next_play)
                    team = team_location_dictionary[next_play_team_poss]

                    plays.at[idx, 'newPossession'] = True
                    plays.at[idx, 'possession'] = team

                    # Save tipoff winner
                    if idx == 1:
                        initial_tip = team

            # Period
            case 'period':
                if subType == 'start':
                    m = PERIOD_RE.search(description)
                    assert m, f"No period found in description: {description}"
                    period = int(m.group(2))
                    quarter_type = m.group(4)

                    if quarter_type == 'Period':
                        if period in (2, 3):
                            plays.at[idx, 'newPossession'] = True
                            plays.at[idx, 'possession'] = get_other_value(initial_tip, teams)

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

        if possession is None:
            last_possession = plays.at[idx - 1, 'possession']
            if possession_end:
                plays.at[idx, 'possession'] = get_other_value(last_possession, teams)
            else:
                plays.at[idx, 'possession'] = last_possession


    # Label possession count
    plays['possessionCount'] = plays['newPossession'].cumsum()

    # Export
    # directory = 'plays'
    # path = os.path.join(directory, f'plays_{key}.csv')
    # plays.to_csv(path)

    # directory2 = 'team_player_dict'
    # os.makedirs(directory2, exist_ok=True)
    # path2 = os.path.join(directory2, f'dict_{key}.json')
    # with open(path2, "w") as f:
    #     json.dump(team_player_dict, f)

    return plays

if __name__ == "__main__":

    with open('game_ids.json', 'r') as f:
        game_ids = json.load(f)

    for key, game_id in tqdm(list(game_ids.items())[:30]):
        get_labelled_play_by_play(game_id=game_id)
        time.sleep(2)
