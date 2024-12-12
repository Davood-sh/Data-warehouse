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
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\Carriers.csv'

try:
    # Load data from the CSV file
    carrier_df = pd.read_csv(csv_file_path)

    # Ensure the required columns exist
    required_columns = {'CODE', 'DESCRIPTION'}
    if not required_columns.issubset(carrier_df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Carrier Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Carrier (
        CODE VARCHAR(10) PRIMARY KEY,
        DESCRIPTION TEXT NOT NULL
    );
    """
    cursor.execute(create_table_query)
    print("Carrier table created successfully.")

    # Insert data into the Carrier Table
    insert_query = """
    INSERT INTO Carrier (CODE, DESCRIPTION) 
    VALUES (%s, %s) 
    ON CONFLICT (CODE) DO NOTHING;
    """
    carrier_records = carrier_df[['CODE', 'DESCRIPTION']].values.tolist()
    cursor.executemany(insert_query, carrier_records)
    print("Carrier table populated successfully.")

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
