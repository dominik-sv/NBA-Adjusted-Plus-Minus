import statsmodels.api as sm
from nba_api.stats.static import players
import pandas as pd
import numpy as np
import os

all_players = players.get_players()
player_id_map = { p['id']: p['full_name'] for p in all_players }

directory = 'data'
df = pd.concat([pd.read_csv(os.path.join(directory, f'data_2024-25_{i}.csv')) for i in range(1, 3)], ignore_index=True)

all_slots = ["P1H","P2H","P3H","P4H","P5H","P1V","P2V","P3V","P4V","P5V"]
players = pd.unique(df[all_slots].values.ravel())

X = pd.DataFrame(0, index=df.index, columns = players)

for i, row in df.iterrows():
    if not np.isnan(row['Net_Rating']):
        for slot in ["P1H","P2H","P3H","P4H","P5H"]:
            player_id = row[slot]
            X.at[i, player_id] = row['Poss_Tot']
        for slot in ["P1V","P2V","P3V","P4V","P5V"]:
            player_id = row[slot]
            X.at[i, player_id] = -row['Poss_Tot']

X = X.loc[(X != 0).any(axis=1), :] 

X_sm = sm.add_constant(X)  
y = df.loc[X.index, 'Net_Rating']
h = df.loc[X.index, 'h']
weights = 1 / h

# Model
wls_model = sm.WLS(y, X_sm, weights=1/h)
if False:
    alpha     = 100
    wls_res = wls_model.fit_regularized(alpha=alpha, L1_wt=0)

wls_res = wls_model.fit()
params = wls_res.params
stderrs = wls_res.bse

# Intercept
intercept = params['const']
inter_se = stderrs['const']

# Player coefficients
player_coefs = params.drop('const')
player_stderrs = stderrs.drop('const')

output_df = pd.DataFrame({
    'Player_Name': [player_id_map.get(pid, pid) for pid in player_coefs.index],
    'Coefficient': player_coefs.values,
    'Std_Error': player_stderrs.values
})
output_df.loc[len(output_df)] = ['Intercept', intercept, inter_se]
output_df.sort_values(by='Coefficient', ascending=False, inplace=True)
output_df.reset_index(drop=True, inplace=True)
output_df = output_df[output_df['Std_Error'] < 5]
output_df.to_csv('player_coefficients.csv', index=False)

top_players = player_coefs.sort_values(ascending=False).iloc[:25].index
for pid in top_players:
    name  = player_id_map.get(pid, pid)
    coef  = player_coefs[pid]
    se    = player_stderrs[pid]
    print(f"{name}: {coef:.3f} ± {se:.3f}")