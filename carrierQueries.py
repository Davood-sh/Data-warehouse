import psycopg2

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Query for delayed flights
    print("Carriers with the Most Delayed Flights (Including Carrier Names):")
    delayed_query = """
        SELECT c.DESCRIPTION AS carrier_name, 
               f.OP_UNIQUE_CARRIER AS carrier_code, 
               COUNT(*) AS total_delays
        FROM Flight f
        INNER JOIN Carrier c ON f.OP_UNIQUE_CARRIER = c.CODE
        WHERE f.DEP_DELAY > 0
        GROUP BY c.DESCRIPTION, f.OP_UNIQUE_CARRIER
        ORDER BY total_delays DESC
        LIMIT 10;
    """
    cursor.execute(delayed_query)
    delayed_results = cursor.fetchall()

    # Print delayed results
    print(f"{'Carrier Name':<30}{'Carrier Code':<15}{'Total Delays':<15}")
    print("-" * 60)
    for row in delayed_results:
        print(f"{row[0]:<30}{row[1]:<15}{row[2]:<15}")

    # Query for canceled flights
    print("\nCarriers with the Most Canceled Flights:")
    canceled_query = """
        SELECT c.DESCRIPTION AS carrier_name,
               f.OP_UNIQUE_CARRIER AS carrier_code,
               COUNT(*) AS total_cancellations
        FROM Flight f
        INNER JOIN Carrier c ON f.OP_UNIQUE_CARRIER = c.CODE
        WHERE f.CANCELLED > 0
        GROUP BY c.DESCRIPTION, f.OP_UNIQUE_CARRIER
        ORDER BY total_cancellations DESC
        LIMIT 10;
    """
    cursor.execute(canceled_query)
    canceled_results = cursor.fetchall()

    # Print canceled results
    print(f"{'Carrier Name':<30}{'Carrier Code':<15}{'Total Cancellations':<20}")
    print("-" * 65)
    for row in canceled_results:
        print(f"{row[0]:<30}{row[1]:<15}{row[2]:<20}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
