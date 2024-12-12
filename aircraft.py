import pandas as pd

# Load your main dataset
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_AllAttributes.csv'
df = pd.read_csv(file_path)

# Define Aircraft-related attributes
aircraft_columns = ['TAIL_NUM', 'MANUFACTURER', 'YEAR OF MANUFACTURE', 'ICAO TYPE', 'RANGE', 'WIDTH']

# Create the Aircraft table by dropping duplicates
aircraft_table = df[aircraft_columns].drop_duplicates().reset_index(drop=True)

# Ensure TAIL_NUM is unique and set it as the primary key
duplicate_tail_num = aircraft_table['TAIL_NUM'].duplicated().sum()
if duplicate_tail_num > 0:
    raise ValueError(f"There are {duplicate_tail_num} duplicate TAIL_NUM entries in the dataset. Please resolve these before proceeding.")

# Save the Aircraft table to a separate file
output_aircraft_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\AircraftTable.csv'
aircraft_table.to_csv(output_aircraft_table_path, index=False)

# Drop Aircraft-related columns from the main table, keeping only TAIL_NUM as the foreign key
df = df.drop(columns=aircraft_columns[1:])  # Exclude TAIL_NUM

# Save the updated main table without Aircraft attributes
output_main_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_UpdatedAircraft.csv'
df.to_csv(output_main_table_path, index=False)

print("Aircraft table and updated main table saved successfully.")

