import duckdb as duck
import pandas as pd

# this is matches from the latest season
# df = pd.read_csv("haaland.csv")
# print(
#     duck.query(
#         """SELECT Round, Result, "Non-Penalty xG",xAG FROM df WHERE competition = 'Premier League'"""
#     ).to_df()
# )
# print(
#     duck.query(
#         """SELECT
#             SUM(Goals),
#             AVG(Goals),
#             SUM("xG"),
#             AVG("xG"),
#             SUM("Non-Penalty xG"),
#             AVG("Non-Penalty xG"),
#             SUM(Assists),
#             SUM(xAG),
#             SUM(Goals) > SUM("Non-Penalty xG") AS over_performing_attacker,
#             SUM(Assists) > SUM(xAG) AS over_performing_assiters
#         FROM df WHERE competition = 'Premier League'
#         """
#     ).to_df()
# )

# print(
#     duck.query(
#         """SELECT
#             Result,
#             Opponent,
#             xG,
#             "Non-Penalty xG",
#             xAG
#         FROM df WHERE competition = 'Premier League'
#         """
#     ).to_df()
# )

player_data = "data/data/2023-24/cleaned_players.csv"
pd.set_option("display.max_rows", None)

df = pd.read_csv(player_data)


# print(
#     duck.query(
#         """
#         WITH gb AS (
#         SELECT
#            first_name,
#            second_name,
#            goals_scored,
#            assists,
#            total_points,
#            minutes,
#            creativity,
#            influence,
#            threat,
#            ict_index,
#            selected_by_percent,
#            now_cost,
#            element_type,
#            ROW_NUMBER() OVER (PARTITION BY element_type ORDER BY ict_index / now_cost DESC ) as row_num
#         FROM df
#         WHERE element_type != 'GK'
#         )

#         SELECT *
#         FROM gb
#         WHERE row_num <= 10
#         """
#     ).to_df()
# )

# print(
#     duck.query(
#         """
#         WITH gb AS (
#         SELECT
#            first_name,
#            second_name,
#            goals_scored,
#            assists,
#            total_points,
#            minutes,
#            creativity,
#            influence,
#            threat,
#            ict_index,
#            selected_by_percent,
#            now_cost,
#            element_type,
#            ROW_NUMBER() OVER (PARTITION BY element_type ORDER BY (total_points*minutes) / now_cost  DESC ) as row_num
#         FROM df
#         WHERE element_type != 'GK'
#         )

#         SELECT *
#         FROM gb
#         WHERE row_num <= 10
#         """
#     ).to_df()
# )

print(
    duck.query(
        """--sql
        WITH gb AS (
        SELECT
            first_name,
            second_name,
            goals_scored,
            assists,
            total_points,
            minutes,
            creativity,
            influence,
            threat,
            ict_index,
            selected_by_percent,
            now_cost,
            element_type,
            ict_index / now_cost as ict_per_cost,
            total_points / now_cost as points_per_cost,
            (ict_index / now_cost) * (total_points / now_cost) as norm
        FROM df
        WHERE ((goals_scored > 1
        AND assists > 1) OR (total_points > 22))
        AND element_type = 'GK'
        )

        SELECT * 
        FROM gb
        ORDER BY norm DESC
        """
    ).to_df()
)
