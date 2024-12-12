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
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\DateTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Ensure the required columns exist
    required_columns = {'FL_DATE', 'DEP_DAY', 'Holiday', 'DEP_WEEK', 'DEP_MONTH', 'DEP_QUARTER', 'DEP_YEAR'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")




    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Geographical Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Date (
        FL_DATE DATE PRIMARY KEY,
        DEP_DAY INT NOT NULL,
        Holiday BOOLEAN NOT NULL,
        DEP_WEEK INT NOT NULL,
        DEP_MONTH INT NOT NULL,
        DEP_QUARTER INT NOT NULL,
        DEP_YEAR INT NOT NULL
    );
    """
    cursor.execute(create_table_query)
    print("Date table created successfully.")

    # Insert data into the Geographical Table
    insert_query = """
    INSERT INTO Date (FL_DATE, DEP_DAY, Holiday, DEP_WEEK, DEP_MONTH, DEP_QUARTER, DEP_YEAR) 
    VALUES (%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (FL_DATE) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Date table populated successfully.")

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
