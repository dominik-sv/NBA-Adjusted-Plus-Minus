from nba_api.stats.endpoints import GameRotation
import pandas as pd

def get_lineups(game_id: str) -> pd.DataFrame:
    """
    Extracts all player lineups for a specific NBA game and their starting and ending times.

    Parameters
    ----------
    game_id : str
        The unique identifier for the NBA game.

    Returns
    -------
    DataFrame
        A DataFrame containing lineup data for the specified game, including player IDs,
        names, and timestamps indicating when each lineup was on the court.
    """

    game_rotation = GameRotation(game_id=game_id).get_data_frames()
    rotation_home, rotation_away = game_rotation[0], game_rotation[1]
    rotation = pd.concat([rotation_home, rotation_away], ignore_index=True)

    rotation.sort_values("OUT_TIME_REAL")

    rotation = rotation[rotation["IN_TIME_REAL"] != rotation["OUT_TIME_REAL"]]

    columns = ["ID", "Start_Time", "End_Time"] + [
        f"Player_{i}_{team}_{label}"
        for team in ["Home", "Away"]
        for i in range(1, 6)
        for label in ["ID", "Name"]
    ]

    lineup = pd.DataFrame(columns=columns)

    times = sorted(rotation["IN_TIME_REAL"].unique()) + [
        rotation["OUT_TIME_REAL"].max()
    ]

    # Starting lineup
    current_lineup = rotation[rotation["IN_TIME_REAL"] == times[0]]
    rotation.to_csv('rotation.csv', index=False)

    # Changing lineups during game
    for t in times:

        next_time = times[times.index(t) + 1]

        # Players in lineup
        player_list = []
        ids_home = []
        ids_visiting = []
        for idx, player in current_lineup.iterrows():
            player_id = player["PERSON_ID"]
            player_name = player["PLAYER_FIRST"] + " " + player["PLAYER_LAST"]
            player_list.extend([player_id, player_name])
            if idx < 5:
                ids_home.append(player_id)
            else:
                ids_visiting.append(player_id)

        ids_home = sorted(ids_home)
        ids_visiting = sorted(ids_visiting)
        id_1 = ''.join(str(id) for id in ids_home)
        id_2 = ''.join(str(id) for id in ids_visiting)
        id = int(''.join([id_1, id_2]))

        new_row = [id, t, next_time] + player_list
        df_row = pd.DataFrame([new_row], columns=lineup.columns)

        if lineup.empty:
            lineup = df_row
        else:
            lineup = pd.concat([lineup, df_row], ignore_index=True)

        if t == times[-2]:
            break

        # Subbing out
        subbing_out_mask = current_lineup["OUT_TIME_REAL"] == next_time
        subbing_out_ids = current_lineup.loc[subbing_out_mask, "PERSON_ID"].tolist()

        # Subbing in
        subbing_in = rotation[rotation["IN_TIME_REAL"] == next_time]
        assert len(subbing_out_ids) == len(
            subbing_in
        ), f"Sub out/in mismatch at t={next_time}, game_id='{game_id}'"

        # Perform subs
        current_lineup = current_lineup[
            ~current_lineup["PERSON_ID"].isin(subbing_out_ids)
        ]
        current_lineup = pd.concat([current_lineup, subbing_in], ignore_index=True)
        assert len(current_lineup) == 10, f"Lineup not complete at t={next_time}"

    return lineup


if __name__ == "__main__":
    lineup = get_lineups()
    print(lineup)

