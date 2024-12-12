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
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\FlightTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)


    # Ensure the required columns exist
    required_columns = {'OP_UNIQUE_CARRIER', 'OP_CARRIER_FL_NUM', 'TAIL_NUM', 'ORIGIN', 'DEST', 'DEP_TIME', 'TAXI_OUT', 'DEP_DELAY', 'AIR_TIME', 'DISTANCE', 'CANCELLED', 'ACTIVE_WEATHER', 'weather_ID'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

        # Reorder the columns in the DataFrame to match the table schema
    df = df[['DEP_TIME', 'TAIL_NUM', 'ORIGIN', 'DEST', 'OP_UNIQUE_CARRIER', 'OP_CARRIER_FL_NUM', 'TAXI_OUT', 'DEP_DELAY', 'AIR_TIME', 'DISTANCE', 'CANCELLED', 'ACTIVE_WEATHER', 'weather_ID']]

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Date_Time Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Flight (
        DEP_TIME TIMESTAMP NOT NULL,        
        TAIL_NUM VARCHAR(10) NOT NULL,
        ORIGIN VARCHAR(10) NOT NULL,
        DEST VARCHAR(10) NOT NULL,
        OP_UNIQUE_CARRIER VARCHAR(10) NOT NULL,
        OP_CARRIER_FL_NUM VARCHAR(10) NOT NULL,
        TAXI_OUT INT NOT NULL,
        DEP_DELAY INT NOT NULL,
        AIR_TIME INT NOT NULL,
        DISTANCE INT NOT NULL,
        CANCELLED INT NOT NULL,
        ACTIVE_WEATHER NUMERIC NOT NULL,
        weather_ID INT NOT NULL,
        PRIMARY KEY (DEP_TIME, TAIL_NUM),
        FOREIGN KEY (DEP_TIME) REFERENCES Date_Time (DEP_TIME),
        FOREIGN KEY (TAIL_NUM) REFERENCES Aircraft (TAIL_NUM),
        FOREIGN KEY (ORIGIN) REFERENCES Station (AIRPORT_CODE),
        FOREIGN KEY (DEST) REFERENCES Station (AIRPORT_CODE),
        FOREIGN KEY (CANCELLED) REFERENCES Cancellation (STATUS),
        FOREIGN KEY (weather_ID) REFERENCES Weather (weather_ID)
    );
    """
    cursor.execute(create_table_query)
    print("Flight table created successfully.")

    # Insert data into the Date_Time Table
    insert_query = """
    INSERT INTO Flight (DEP_TIME, TAIL_NUM, ORIGIN, DEST, OP_UNIQUE_CARRIER, OP_CARRIER_FL_NUM, TAXI_OUT, DEP_DELAY, AIR_TIME, DISTANCE, CANCELLED, ACTIVE_WEATHER, weather_ID) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (DEP_TIME, TAIL_NUM) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Flight table populated successfully.")

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
