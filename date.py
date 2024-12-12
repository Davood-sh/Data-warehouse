import pandas as pd

# Load the Time Table
time_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\TimeTable.csv'
time_df = pd.read_csv(time_table_path)

# Check for duplicates in FL_DATE
if time_df['FL_DATE'].duplicated().any():
    print("Duplicate FL_DATE values found in Time Table. Removing duplicates for Date Table.")
else:
    print("No duplicates found in FL_DATE.")

# Create the Date Table ensuring consistent values for Holiday
date_table = (
    time_df.groupby('FL_DATE', as_index=False)
    .agg({
        'DEP_DAY': 'first',
        'Holiday': 'max',  # Use max to ensure True if any row has True
        'DEP_WEEK': 'first',
        'DEP_MONTH': 'first',
        'DEP_QUARTER': 'first',
        'DEP_YEAR': 'first'
    })
)


# Save the Date Table
date_table_output_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\DateTable.csv'
date_table.to_csv(date_table_output_path, index=False)
print(f"Date Table saved at: {date_table_output_path}")

# Update the Time Table by dropping date-related attributes and DEP_HOUR
time_columns_to_drop = ['DEP_DAY', 'Holiday', 'DEP_WEEK', 'DEP_MONTH', 'DEP_QUARTER', 'DEP_YEAR', 'DEP_HOUR']
time_df = time_df.drop(columns=time_columns_to_drop)

# Save the updated Time Table
time_table_output_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\UpdatedTimeTable.csv'
time_df.to_csv(time_table_output_path, index=False)
print(f"Updated Time Table saved at: {time_table_output_path}")
