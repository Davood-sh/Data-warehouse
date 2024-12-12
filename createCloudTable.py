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
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\CloudTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Ensure the required columns exist
    required_columns = {'CLOUD_COVER', 'HIGH_LEVEL_CLOUD', 'MID_LEVEL_CLOUD', 'LOW_LEVEL_CLOUD', 'N_CLOUD_LAYER', 'LOWEST_CLOUD_LAYER', 'cloud_ID'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

    #fill missing values with a default value (e.g., 0)
    df.fillna({'CLOUD_COVER': 0, 'HIGH_LEVEL_CLOUD': 0, 'MID_LEVEL_CLOUD': 0, 'LOW_LEVEL_CLOUD': 0, 'N_CLOUD_LAYER': 0, 'LOWEST_CLOUD_LAYER': 0}, inplace=True)

    # Reorder the columns in the DataFrame to match the table schema
    df = df[['cloud_ID', 'CLOUD_COVER', 'HIGH_LEVEL_CLOUD', 'MID_LEVEL_CLOUD', 'LOW_LEVEL_CLOUD', 'N_CLOUD_LAYER', 'LOWEST_CLOUD_LAYER']]

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Geographical Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Cloud (
        cloud_ID INT PRIMARY KEY,
        CLOUD_COVER INT NOT NULL,
        HIGH_LEVEL_CLOUD INT NOT NULL,
        MID_LEVEL_CLOUD INT NOT NULL,
        LOW_LEVEL_CLOUD INT NOT NULL,
        N_CLOUD_LAYER INT NOT NULL,
        LOWEST_CLOUD_LAYER INT NOT NULL
    );
    """
    cursor.execute(create_table_query)
    print("Cloud table created successfully.")

    # Insert data into the Geographical Table
    insert_query = """
    INSERT INTO Cloud (cloud_ID, CLOUD_COVER, HIGH_LEVEL_CLOUD, MID_LEVEL_CLOUD, LOW_LEVEL_CLOUD, N_CLOUD_LAYER, LOWEST_CLOUD_LAYER) 
    VALUES (%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (cloud_ID) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Cloud table populated successfully.")

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