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
csv_file_path = r'C:\Users\Asus\Desktop\D2\Master\Data Management\Project\2022 US Airlines Domestic Departure Data\UpdatedTimeTable.csv'

try:
    # Load data from the CSV file
    df = pd.read_csv(csv_file_path)

    # Ensure the required columns exist
    required_columns = {'DEP_TIME', 'FL_DATE', 'DEP_TIME_ONLY'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"The CSV file must contain the following columns: {required_columns}")

   

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the Date_Time Table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Date_Time (
        DEP_TIME TIMESTAMP PRIMARY KEY,
        FL_DATE DATE NOT NULL,
        DEP_TIME_ONLY TIME NOT NULL,
        FOREIGN KEY (FL_DATE) REFERENCES Date (FL_DATE)
    );
    """
    cursor.execute(create_table_query)
    print("Date_Time table created successfully.")

    # Insert data into the Date_Time Table
    insert_query = """
    INSERT INTO Date_Time (DEP_TIME, FL_DATE, DEP_TIME_ONLY) 
    VALUES (%s, %s, %s) 
    ON CONFLICT (DEP_TIME) DO NOTHING;
    """
    records = df.values.tolist()  # Extract reordered data
    cursor.executemany(insert_query, records)
    print("Date_Time table populated successfully.")

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
