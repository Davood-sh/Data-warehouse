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

# Path to the Aircraft.csv file
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\AircraftTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Rename columns to match PostgreSQL table schema
    df.rename(columns={
        'YEAR OF MANUFACTURE': 'YEAR_OF_MANUFACTURE',
        'ICAO TYPE': 'ICAO_TYPE'
    }, inplace=True)

    # Ensure the required columns exist
    required_columns = {'TAIL_NUM', 'MANUFACTURER', 'YEAR_OF_MANUFACTURE', 'ICAO_TYPE', 'RANGE', 'WIDTH'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Aircraft Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Aircraft (
        TAIL_NUM VARCHAR(10) PRIMARY KEY,
        MANUFACTURER VARCHAR(50) NOT NULL,
        YEAR_OF_MANUFACTURE INT NOT NULL,
        ICAO_TYPE VARCHAR(10) NOT NULL,
        RANGE TEXT,
        WIDTH TEXT
    );
    """
    cursor.execute(create_table_query)
    print("Aircraft table created successfully.")

    # Insert data into the Aircraft Table
    insert_query = """
    INSERT INTO Aircraft (TAIL_NUM, MANUFACTURER, YEAR_OF_MANUFACTURE, ICAO_TYPE, RANGE, WIDTH) 
    VALUES (%s, %s, %s, %s, %s, %s) 
    ON CONFLICT (TAIL_NUM) DO NOTHING;
    """
    records = df[['TAIL_NUM', 'MANUFACTURER', 'YEAR_OF_MANUFACTURE', 'ICAO_TYPE', 'RANGE', 'WIDTH']].values.tolist()
    cursor.executemany(insert_query, records)
    print("Aircraft table populated successfully.")

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
