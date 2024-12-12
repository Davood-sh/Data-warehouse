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

# Path to the Geographical.csv file
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\GeographicalTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Ensure the required columns exist
    required_columns = {'LATITUDE', 'LONGITUDE', 'ELEVATION', 'geographical_ID'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

    # Reorder the columns in the DataFrame to match the table schema
    df = df[['geographical_ID', 'LATITUDE', 'LONGITUDE', 'ELEVATION']]

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Geographical Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Geographical (
        geographical_ID INT PRIMARY KEY,
        LATITUDE NUMERIC NOT NULL,
        LONGITUDE NUMERIC NOT NULL,
        ELEVATION NUMERIC NOT NULL
    );
    """
    cursor.execute(create_table_query)
    print("Geographical table created successfully.")

    # Insert data into the Geographical Table
    insert_query = """
    INSERT INTO Geographical (geographical_ID, LATITUDE, LONGITUDE, ELEVATION) 
    VALUES (%s, %s, %s, %s) 
    ON CONFLICT (geographical_ID) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Geographical table populated successfully.")

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
