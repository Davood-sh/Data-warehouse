import pandas as pd

# Load your dataset
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_DEP_TIME_ONLY.csv'
df = pd.read_csv(file_path)

# Check for duplicates
duplicates = df.duplicated()  # This returns a boolean Series

# Count the number of duplicates
duplicate_count = duplicates.sum()

print("Total number of duplicate rows:", duplicate_count)

# Optionally, display the duplicate rows
if duplicate_count > 0:
    print("Duplicate rows:")
    print(df[duplicates])
