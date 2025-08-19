import statsmodels.api as sm
from nba_api.stats.static import players
import pandas as pd
import numpy as np
import os
from tqdm import tqdm

all_players = players.get_players()
player_id_map = { p['id']: p['full_name'] for p in all_players }

directory = 'data'
print('Importing')
df = pd.concat([pd.read_csv(os.path.join(directory, f'data_20{year}-{year + 1}_{i}.csv')) for i in range(1, 12) for year in range(22, 25)], ignore_index=True)

all_slots = ["P1H","P2H","P3H","P4H","P5H","P1V","P2V","P3V","P4V","P5V"]
plyrs = pd.unique(df[all_slots].values.ravel())

X = pd.DataFrame(0, index=df.index, columns = plyrs)
player_sample_size = {}

for i, row in tqdm(df.iterrows(), desc = "Processing lineups"):
    if not np.isnan(row['Net_Rating']):
        for slot in ["P1H","P2H","P3H","P4H","P5H"]:
            player_id = row[slot]
            player_sample_size[player_id] = player_sample_size.get(player_id, 0) + row['Poss_Tot']
            X.at[i, player_id] = +1
        for slot in ["P1V","P2V","P3V","P4V","P5V"]:
            player_id = row[slot]
            player_sample_size[player_id] = player_sample_size.get(player_id, 0) + row['Poss_Tot']
            X.at[i, player_id] = -1

mask = (X != 0).any(axis=1) & (df['h'] > 0)

X = X.loc[mask]
y = df.loc[mask, 'Net_Rating']
w = 1 / df.loc[mask, 'h']

from sklearn.linear_model import RidgeCV
alphas = np.logspace(-2, 4, 50)
model = RidgeCV(alphas=alphas, fit_intercept=False, cv=5, scoring='neg_mean_squared_error')
model.fit(X.values, y.values, sample_weight=w.values)

player_coefs = pd.Series(model.coef_, index=X.columns)

# ref_pid   = X.columns[-1]
# X_ref     = X.drop(columns=[ref_pid])
# X_sm_ref  = sm.add_constant(X_ref)

# alpha     = 100

# # Weights
# w_sqrt = np.sqrt(weights.values)
# Xw = X_sm_ref.values * w_sqrt[:, None]
# yw = y.values * w_sqrt

# # Prepare penalization
# p = X_sm_ref.shape[1]
# pen_mask = np.ones(p, dtype=float)
# pen_mask[0] = 0.0

# # Augment rows
# X_aug = np.vstack([Xw, np.sqrt(alpha) * np.diag(pen_mask)])
# y_aug = np.concatenate([yw, np.zeros(p)])

# # OLS on augmented system
# print("Fitting model...")
# ols_aug = sm.OLS(y_aug, X_aug).fit()

# params = pd.Series(ols_aug.params, index = X_sm_ref.columns)
# stderrs = pd.Series(ols_aug.bse, index = X_sm_ref.columns)

# # Intercept
# intercept = params['const']
# inter_se = stderrs['const']

# # Player coefficients
# player_coefs = params.drop('const').copy()
# player_stderrs = stderrs.drop('const').copy()

# player_coefs[ref_pid] = -player_coefs.sum()
# player_stderrs[ref_pid] = np.nan

output_df = pd.DataFrame({
    'Player_Name': [player_id_map.get(pid, pid) for pid in player_coefs.index],
    'Coefficient': player_coefs.values,
    # 'Std_Error': player_stderrs.values,
    'Possessions': [player_sample_size.get(pid, 0) for pid in player_coefs.index]
})

# output_df.loc[len(output_df)] = ['Intercept', intercept, inter_se, 0]

output_df.sort_values(by='Coefficient', ascending=False, inplace=True)
output_df.reset_index(drop=True, inplace=True)
output_df = output_df[output_df['Possessions'] > 3000]

print("Exporting...")
output_df.to_csv('player_coefficients.csv', index=False)