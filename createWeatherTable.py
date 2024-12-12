import psycopg2
import pandas as pd

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Path to the Date_Time.csv file
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\WeatherTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)


    # Ensure the required columns exist
    required_columns = {'ALTIMETER', 'REL_HUMIDITY', 'DEW_POINT', 'TEMPERATURE', 'VISIBILITY', 'cloud_ID', 'wind_ID', 'weather_ID'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

        # Reorder the columns in the DataFrame to match the table schema
    df = df[['weather_ID', 'ALTIMETER', 'REL_HUMIDITY', 'DEW_POINT', 'TEMPERATURE', 'VISIBILITY', 'cloud_ID', 'wind_ID']]

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Date_Time Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Weather (
        weather_ID INT PRIMARY KEY,        
        ALTIMETER NUMERIC NOT NULL,
        REL_HUMIDITY NUMERIC NOT NULL,
        DEW_POINT NUMERIC NOT NULL,
        TEMPERATURE NUMERIC NOT NULL,
        VISIBILITY NUMERIC NOT NULL,
        cloud_ID INT NOT NULL,
        wind_ID INT NOT NULL,
        FOREIGN KEY (cloud_ID) REFERENCES Cloud (cloud_ID),
        FOREIGN KEY (wind_ID) REFERENCES Wind (wind_ID)
    );
    """
    cursor.execute(create_table_query)
    print("Weather table created successfully.")

    # Insert data into the Date_Time Table
    insert_query = """
    INSERT INTO Weather (weather_ID, ALTIMETER, REL_HUMIDITY, DEW_POINT, TEMPERATURE, VISIBILITY, cloud_ID, wind_ID) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (weather_ID) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Weather table populated successfully.")

    # Commit the changes
    conn.commit()

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
