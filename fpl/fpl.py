# open soccer-spi/spi_matches_latest.csv using pandas

import pandas as pd


# this is matches from the latest season
df = pd.read_csv('../soccer-spi/spi_matches_latest.csv')

# filter out matches that are not in the premier league
df = df[df['league_id'] == 2411]

# rename some columns
# team1 = home_team
# team2 = away_team
# spi1 = home_team_rating
# spi2 = away_team_rating
# prob1 = home_team_win_probability
# prob2 = away_team_win_probability
# probtie = tie_probability
# proj_score1 = home_team_projected_score
# proj_score2 = away_team_projected_score
# importance1 = home_team_importance
# importance2 = away_team_importance
# score1 = home_team_score
# score2 = away_team_score
# xg1 = home_team_expected_goals
# xg2 = away_team_expected_goals
# nsxg1 = home_team_non_shot_expected_goals
# nsxg2 = away_team_non_shot_expected_goals
# adj_score1 = home_team_adjusted_score
# adj_score2 = away_team_adjusted_score

df = df[(df['team1'] == 'Manchester City') | (df['team2'] == 'Manchester City')]

# expect 38 matches, expect last game to be meaningless

# remove season, date, league_id, and league from the dataframe
df = df.drop(columns=['season', 'date', 'league_id', 'league'])

print(df)

