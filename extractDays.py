import pandas as pd
import holidays

# Load your dataset
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\Delayed_Cancelled.csv'
df = pd.read_csv(file_path)

# Create a US Federal Holiday Calendar for 2022
us_holidays_2022 = holidays.US(years=2022)

# Convert the FL_DATE column to datetime, if it isn't already
df['DEP_TIME'] = pd.to_datetime(df['DEP_TIME'])

# Add a "Holiday" column: True if the date is a holiday, False otherwise
df['Holiday'] = df['DEP_TIME'].apply(lambda x: x in us_holidays_2022)

# Derive day, month, quarter, and year from the DEP_TIME column
# Ensure DEP_TIME is in datetime format; first handle missing values and non-standard entries
df['DEP_TIME'] = pd.to_datetime(df['DEP_TIME'], errors='coerce')


# Extract time, day, month, quarter, year, and week
df['DEP_TIME_ONLY'] = df['DEP_TIME'].dt.time  # Extract the time (HH:MM:SS)
df['DEP_DAY'] = df['DEP_TIME'].dt.day  # Day of the month (1-31)
df['DEP_WEEK'] = df['DEP_TIME'].dt.isocalendar().week  # Week of the year (1-52/53)
df['DEP_MONTH'] = df['DEP_TIME'].dt.month  # Month (1-12)
df['DEP_QUARTER'] = df['DEP_TIME'].dt.quarter  # Quarter of the year (1-4)
df['DEP_YEAR'] = df['DEP_TIME'].dt.year.fillna(2022).astype(int)  # Year (default 2022 if missing)


# Save the updated DataFrame with new columns
output_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_DerivedTimeColumns.csv'
df.to_csv(output_path, index=False)

# Check the result
print(df[['DEP_TIME','DEP_TIME_ONLY' ,'DEP_DAY', 'DEP_WEEK','DEP_MONTH', 'DEP_QUARTER', 'DEP_YEAR', 'Holiday']].head())
