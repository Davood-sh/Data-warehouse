import pandas as pd

# Load your Station Table
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_AllAttributes.csv'
df = pd.read_csv(file_path)

# List the columns you want to drop
weather_columns = ['ALTIMETER', 'REL_HUMIDITY', 'DEW_POINT', 'TEMPERATURE', 'VISIBILITY']
cloud_columns = ['CLOUD_COVER', 'HIGH_LEVEL_CLOUD', 'MID_LEVEL_CLOUD', 'LOW_LEVEL_CLOUD', 'N_CLOUD_LAYER', 'LOWEST_CLOUD_LAYER', 'cloud_ID']
wind_columns = ['WIND_GUST', 'WIND_SPD', 'WIND_DIR', 'wind_ID']
aircraft_columns = ['MANUFACTURER', 'YEAR OF MANUFACTURE', 'ICAO TYPE', 'RANGE', 'WIDTH']
geographical_columns = ['LATITUDE', 'LONGITUDE', 'ELEVATION']
time_columns = ['FL_DATE', 'DEP_TIME_ONLY', 'DEP_HOUR', 'DEP_DAY', 'Holiday', 'DEP_WEEK', 'DEP_MONTH', 'DEP_QUARTER', 'DEP_YEAR']
general_columns = ['MKT_UNIQUE_CARRIER', 'MKT_CARRIER_FL_NUM', 'CRS_DEP_TIME','MESONET_STATION']

# Drop these columns from the main station table
df.drop(columns=weather_columns + cloud_columns + wind_columns + aircraft_columns + geographical_columns + time_columns + general_columns, inplace=True)

# Save the updated station table after dropping the specified columns
output_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\FlightTable.csv'
df.to_csv(output_path, index=False)

# Print a success message
print(f"Updated Station Table after dropping specified columns saved at: {output_path}")
