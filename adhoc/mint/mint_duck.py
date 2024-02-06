import duckdb as duck
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

# need transactions.csv from mint

print("Spent at WF")

print(
    duck.query(
        """
        SELECT
            month(Date)::text || '/' || year(Date)::text as monthyear,
            sum(Amount) 
        FROM read_csv_auto('transactions.csv', dateformat = '%m/%d/%Y')
        WHERE lower(category) NOT IN ('credit', 'paycheck', 'deposit') 
        AND Description LIKE '%WHOLE%'
        AND Date >= '2023-10-01'
        GROUP BY 1
        ORDER BY 2 DESC
        """
    ).to_df()
)

print(
    duck.query(
        """
        SELECT 
            category,
            sum(Amount) / 6 AS monthly_avg
        FROM read_csv_auto('transactions.csv', dateformat = '%m/%d/%Y')
        WHERE (lower(category) IN ('shopping', 'restaurants', 'groceries', 'coffee shops', 'clothing')
        OR category LIKE '%food%')
        AND Date >= '2023-08-01'
        GROUP BY 1
        ORDER BY 2
        """
    ).to_df()
)

print(
    duck.query(
        """
         SELECT
            sum(Amount) / 6 AS monthly_avg
        FROM read_csv_auto('transactions.csv', dateformat = '%m/%d/%Y')
        WHERE lower(category) NOT IN ('credit', 'paycheck', 'deposit') 
        AND Description NOT LIKE '%Payment%'
        AND Date >= '2023-08-01'
        """
    ).to_df()
)
