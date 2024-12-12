import pandas as pd

# Load the CSV file
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_DEP_TIME_ONLY.csv'
df = pd.read_csv(file_path)

# Separate 100 rows from the data
subset_data = df.head(100)  # Adjust to slice any 100 rows you want, e.g., data.iloc[:100] for first 100 rows
output_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\subset_100row.csv'
subset_data.to_csv(output_file_path, index=False)

"""
filtered_df = df[
    (df['CANCELLED'] > 0) | (df['DEP_DELAY'] > 0)
]

row_count = len(filtered_df)

# Display the separated data
print(row_count)

# Define the path where you want to save the subset
output_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\Delayed_Cancelled.csv'
filtered_df.to_csv(output_file_path, index=False)
"""