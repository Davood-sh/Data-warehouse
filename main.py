import psycopg2

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Query to find carriers with the most delayed flights (including carrier description)
query = """
    SELECT c.DESCRIPTION AS carrier_name, f.OP_UNIQUE_CARRIER AS carrier_code, COUNT(*) AS total_delays
    FROM Flight f
    INNER JOIN Carrier c ON f.OP_UNIQUE_CARRIER = c.CODE
    WHERE f.DEP_DELAY > 0
    GROUP BY c.DESCRIPTION, f.OP_UNIQUE_CARRIER
    ORDER BY total_delays DESC
    LIMIT 10;
"""

# Main function to execute the query
def main():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Execute the query
        print("Carriers with the Most Delayed Flights (Including Carrier Names):")
        cursor.execute(query)
        results = cursor.fetchall()

        # Print the results
        print(f"{'Carrier Name':<30}{'Carrier Code':<15}{'Total Delays':<15}")
        print("-" * 60)
        for row in results:
            print(f"{row[0]:<30}{row[1]:<15}{row[2]:<15}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Entry point
if __name__ == "__main__":
    main()
