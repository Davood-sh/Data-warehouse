import pandas as pd

# Load your dataset
file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_DerivedTimeColumns.csv'
df = pd.read_csv(file_path)

# Define columns for weather, cloud, wind, and geographical dimensions
weather_columns = ['ALTIMETER', 'REL_HUMIDITY', 'DEW_POINT', 'TEMPERATURE', 'VISIBILITY']
cloud_columns = ['CLOUD_COVER', 'HIGH_LEVEL_CLOUD', 'MID_LEVEL_CLOUD', 'LOW_LEVEL_CLOUD', 'N_CLOUD_LAYER', 'LOWEST_CLOUD_LAYER']
wind_columns = ['WIND_GUST', 'WIND_SPD', 'WIND_DIR']


# Create unique dimension tables for cloud and wind first
cloud_table = df[cloud_columns].drop_duplicates().reset_index(drop=True)
cloud_table['cloud_ID'] = cloud_table.index + 1

wind_table = df[wind_columns].drop_duplicates().reset_index(drop=True)
wind_table['wind_ID'] = wind_table.index + 1

# Merge cloud_ID and wind_ID into the main dataframe to associate with weather
df = df.merge(cloud_table, on=cloud_columns, how='left')
df = df.merge(wind_table, on=wind_columns, how='left')

# Create the weather table, which includes cloud_ID and wind_ID as foreign keys
weather_columns_with_ids = weather_columns + ['cloud_ID', 'wind_ID']
weather_table = df[weather_columns_with_ids].drop_duplicates().reset_index(drop=True)
weather_table['weather_ID'] = weather_table.index + 1



# Merge the weather_ID and geographical_ID back into the main table
df = df.merge(weather_table, on=weather_columns_with_ids, how='left')


# Save the main table with all attributes (before dropping redundant columns)
output_main_with_all_attributes_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_with_AllAttributes.csv'
df.to_csv(output_main_with_all_attributes_path, index=False)

# Drop redundant columns from the main table after normalizing
df = df.drop(columns=weather_columns + cloud_columns + wind_columns)

# Save the new main table and the separate dimension tables
output_main_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CompleteData_UpdatedWeather.csv'
output_weather_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\WeatherTable.csv'
output_cloud_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CloudTable.csv'
output_wind_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\WindTable.csv'


df.to_csv(output_main_table_path, index=False)
weather_table.to_csv(output_weather_table_path, index=False)
cloud_table.to_csv(output_cloud_table_path, index=False)
wind_table.to_csv(output_wind_table_path, index=False)


print("Main table with all attributes saved successfully.")
print("Main table with IDs and separate dimension tables saved successfully.")
