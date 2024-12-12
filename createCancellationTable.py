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

# Path to the Carriers.csv file
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\Cancellation.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Ensure the required columns exist
    required_columns = {'STATUS', 'CANCELLATION_REASON'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Carrier Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Cancellation (
        STATUS INT PRIMARY KEY,
        CANCELLATION_REASON TEXT NOT NULL
    );
    """
    cursor.execute(create_table_query)
    print("Cancellation table created successfully.")

    # Insert data into the Carrier Table
    insert_query = """
    INSERT INTO Cancellation (STATUS, CANCELLATION_REASON) 
    VALUES (%s, %s) 
    ON CONFLICT (STATUS) DO NOTHING;
    """
    records = df[['STATUS', 'CANCELLATION_REASON']].values.tolist()
    cursor.executemany(insert_query, records)
    print("Cancellation table populated successfully.")

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
