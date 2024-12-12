import pandas as pd

# Load your Station Table
station_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\Stations.csv'
station_df = pd.read_csv(station_file_path)

# Define geographical attributes
geographical_columns = ['LATITUDE', 'LONGITUDE', 'ELEVATION']
general_columns=['AIRPORT_STATE_CODE', 'ICAO', 'IATA', 'FAA', 'MESONET_STATION']
# Create the Geographical Table by dropping duplicates
geographical_table = station_df[geographical_columns].drop_duplicates().reset_index(drop=True)

# Add a unique geographical_ID as the primary key
geographical_table['geographical_ID'] = geographical_table.index + 1

# Merge geographical_ID into the Station Table
station_df = station_df.merge(geographical_table, on=geographical_columns, how='left')

# Save the Station with Geogerafical Id Table
output_station_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\StationWithGeoId.csv'
station_df.to_csv(output_station_table_path, index=False)

# Drop the original geographical attributes from the Station Table
station_df = station_df.drop(columns=geographical_columns + general_columns)

# Save the Geographical Table
output_geographical_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\GeographicalTable.csv'
geographical_table.to_csv(output_geographical_table_path, index=False)

# Save the new Station Table
output_station_table_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\UpdatedStationTable.csv'
station_df.to_csv(output_station_table_path, index=False)

print("Geographical Table and updated Station Table saved successfully.")
