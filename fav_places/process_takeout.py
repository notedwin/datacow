import os

import pandas as pd

# Set the path to the folder containing CSV files
folder_path = "../Takeout/Saved/"

# Set the path for the output Excel file
output_excel_path = "saved_places.xlsx"

# Get a list of all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

dfs = []

# Loop through each CSV file and read it into a DataFrame, then append to the list
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    df = pd.read_csv(file_path)
    dfs.append(df)

# Concatenate all DataFrames in the list along rows
merged_data = pd.concat(dfs, ignore_index=True)

# Write the merged data to an Excel file
merged_data.to_excel(output_excel_path, index=False)

print(f"Merged data saved to {output_excel_path}")
