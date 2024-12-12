import pandas as pd

# Load your dataset
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_AllAttributes.csv'
df = pd.read_csv(file_path)

# Ensure DEP_TIME is in datetime format
df['DEP_TIME'] = pd.to_datetime(df['DEP_TIME'])
df['FL_DATE'] = pd.to_datetime(df['FL_DATE'])  # If not already in datetime format

# Create Time Table from existing time attributes
time_table = df[['DEP_TIME', 'FL_DATE', 'DEP_TIME_ONLY', 'DEP_HOUR', 'DEP_DAY', 'Holiday','DEP_WEEK', 'DEP_MONTH', 'DEP_QUARTER', 'DEP_YEAR']]

# Check for duplicates in DEP_TIME
duplicate_dep_time = time_table['DEP_TIME'].duplicated().sum()
if duplicate_dep_time > 0:
    print(f"Warning: Found {duplicate_dep_time} duplicate DEP_TIME entries in the dataset!")
    time_table = time_table.drop_duplicates(subset='DEP_TIME')  # Keep only unique DEP_TIME rows

# Save the Time Table to a CSV file
time_table_output_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\TimeTable.csv'
time_table.to_csv(time_table_output_path, index=False)

# Drop time attributes from the main table except DEP_TIME (foreign key)
df = df.drop(columns=['FL_DATE', 'DEP_TIME_ONLY', 'DEP_HOUR', 'DEP_DAY', 'Holiday','DEP_WEEK', 'DEP_MONTH', 'DEP_QUARTER', 'DEP_YEAR'])

# Save the updated main table
main_table_output_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_UpdatedTime.csv'
df.to_csv(main_table_output_path, index=False)

print("Time Table and updated main table have been saved successfully.")
