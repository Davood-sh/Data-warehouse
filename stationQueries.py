import psycopg2

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Queries
queries = {
    "busiest_airports": """
        SELECT 
            s.AIRPORT_CODE,
            s.AIRPORT_NAME,
            s.AIRPORT_CITY,
            s.AIRPORT_STATE,
            COUNT(*) AS total_flights
        FROM Flight f
        JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE OR f.DEST = s.AIRPORT_CODE
        GROUP BY s.AIRPORT_CODE, s.AIRPORT_NAME, s.AIRPORT_CITY, s.AIRPORT_STATE
        ORDER BY total_flights DESC
        LIMIT 10;
    """,
    "airports_with_most_canceled_flights": """
        SELECT 
            s.AIRPORT_CODE,
            s.AIRPORT_NAME,
            s.AIRPORT_CITY,
            s.AIRPORT_STATE,
            COUNT(f.CANCELLED) AS canceled_flights
        FROM Flight f
        JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE
        WHERE f.CANCELLED > 0
        GROUP BY s.AIRPORT_CODE, s.AIRPORT_NAME, s.AIRPORT_CITY, s.AIRPORT_STATE
        ORDER BY canceled_flights DESC
        LIMIT 10;
    """,
    "airports_with_most_delayed_flights": """
        SELECT 
            s.AIRPORT_CODE,
            s.AIRPORT_NAME,
            s.AIRPORT_CITY,
            s.AIRPORT_STATE,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE
        WHERE f.DEP_DELAY > 0
        GROUP BY s.AIRPORT_CODE, s.AIRPORT_NAME, s.AIRPORT_CITY, s.AIRPORT_STATE
        ORDER BY delayed_flights DESC
        LIMIT 10;
    """,
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Execute and print results for each query
    for query_name, query in queries.items():
        print(f"\nResults for {query_name.replace('_', ' ').title()}:")
        cursor.execute(query)
        results = cursor.fetchall()

        # Determine column headers based on query
        if query_name == "busiest_airports":
            headers = ["Airport Code", "Airport Name", "City", "State", "Total Flights"]
        elif query_name == "airports_with_most_canceled_flights":
            headers = ["Airport Code", "Airport Name", "City", "State", "Canceled Flights"]
        elif query_name == "airports_with_most_delayed_flights":
            headers = ["Airport Code", "Airport Name", "City", "State", "Delayed Flights"]

        # Print the headers
        print(" | ".join(f"{header:<20}" for header in headers))
        print("-" * (20 * len(headers)))

        # Print the rows
        for row in results:
            print(" | ".join(f"{str(value):<20}" for value in row))

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
