import psycopg2
import os

# Define database connection details
conn_details = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Directory to save CSV files
output_dir = r"E:\flight_analysis"  # Use raw string for Windows paths
os.makedirs(output_dir, exist_ok=True)

# Query to get all table names
query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
"""

try:
    # Establish a connection to the database
    conn = psycopg2.connect(**conn_details)
    with conn.cursor() as cursor:
        # Execute the query to fetch all table names
        cursor.execute(query)
        tables = cursor.fetchall()

        # Loop through each table and export it to a CSV file
        for table in tables:
            table_name = table[0]
            output_file = os.path.join(output_dir, f"{table_name}.csv")
            with open(output_file, "w") as f:
                cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", f)
            print(f"Exported {table_name} to {output_file}")

finally:
    # Close the connection
    conn.close()
