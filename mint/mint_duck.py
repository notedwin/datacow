import duckdb as duck
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

df = pd.read_csv("transactions.csv")
# print(
#     duck.query(
#         """SELECT 'discover',sum(Amount) FROM df WHERE lower(Description) LIKE '%discover%'"""
#     ).to_df()
# )

# Payment Thank You-Mobile
# payment to Chase


print(
    duck.query(
        """SELECT Description,sum(Amount) FROM df WHERE lower(category) NOT IN ('credit', 'paycheck', 'deposit') GROUP BY 1 ORDER BY 2"""
    ).to_df()
)

# print(
#     duck.query(
#         """SELECT Description, SUM(Amount) FROM df GROUP BY 1 ORDER BY 2"""
#     ).to_df()
# )
