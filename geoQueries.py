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
    "geographical_area_with_most_delayed_flights": """
        SELECT 
            g.geographical_ID,
            s.AIRPORT_NAME,
            s.AIRPORT_CITY,
            s.AIRPORT_STATE,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE
        JOIN Geographical g ON s.geographical_ID = g.geographical_ID
        WHERE f.DEP_DELAY > 0
        GROUP BY g.geographical_ID, s.AIRPORT_NAME, s.AIRPORT_CITY, s.AIRPORT_STATE
        ORDER BY delayed_flights DESC
        LIMIT 10;
    """,
    "geographical_area_with_most_canceled_flights": """
        SELECT 
            g.geographical_ID,
            s.AIRPORT_NAME,
            s.AIRPORT_CITY,
            s.AIRPORT_STATE,
            COUNT(f.CANCELLED) AS canceled_flights
        FROM Flight f
        JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE
        JOIN Geographical g ON s.geographical_ID = g.geographical_ID
        WHERE f.CANCELLED > 0
        GROUP BY g.geographical_ID, s.AIRPORT_NAME, s.AIRPORT_CITY, s.AIRPORT_STATE
        ORDER BY canceled_flights DESC
        LIMIT 10;
    """
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

        # Print headers
        headers = ["Geographical ID", "Airport Name", "City", "State", "Count"]
        print(" | ".join(f"{header:<20}" for header in headers))
        print("-" * (20 * len(headers)))

        # Print each row
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
