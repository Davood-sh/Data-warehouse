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
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\UpdatedStationTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Rename columns to match PostgreSQL table schema
    df.rename(columns={
        'AIRPORT': 'AIRPORT_CODE',
        'DISPLAY_AIRPORT_NAME': 'AIRPORT_NAME',
        'DISPLAY_AIRPORT_CITY_NAME_FULL': 'AIRPORT_CITY',
        'AIRPORT_STATE_NAME': 'AIRPORT_STATE'
    }, inplace=True)

    # Ensure the required columns exist
    required_columns = {'AIRPORT_ID', 'AIRPORT_CODE', 'AIRPORT_NAME', 'AIRPORT_CITY', 'AIRPORT_STATE', 'geographical_ID'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

        # Reorder the columns in the DataFrame to match the table schema
    df = df[['AIRPORT_CODE', 'AIRPORT_ID', 'AIRPORT_NAME', 'AIRPORT_CITY', 'AIRPORT_STATE', 'geographical_ID']]

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Date_Time Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Station (
        AIRPORT_CODE VARCHAR(10) PRIMARY KEY,
        AIRPORT_ID INT NOT NULL,
        AIRPORT_NAME TEXT NOT NULL,
        AIRPORT_CITY TEXT NOT NULL,
        AIRPORT_STATE TEXT NOT NULL,
        geographical_ID INT NOT NULL,
        FOREIGN KEY (geographical_ID) REFERENCES Geographical (geographical_ID)
    );
    """
    cursor.execute(create_table_query)
    print("Station table created successfully.")

    # Insert data into the Date_Time Table
    insert_query = """
    INSERT INTO Station (AIRPORT_CODE, AIRPORT_ID, AIRPORT_NAME, AIRPORT_CITY, AIRPORT_STATE, geographical_ID) 
    VALUES (%s, %s, %s, %s, %s, %s) 
    ON CONFLICT (AIRPORT_CODE) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Station table populated successfully.")

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
