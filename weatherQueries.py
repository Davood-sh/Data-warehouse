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
    "weather_impact_on_delays": """
        SELECT w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER, COUNT(f.DEP_DELAY) AS delay_count
        FROM Flight f
        JOIN Weather w ON f.weather_ID = w.weather_ID
        JOIN Cloud c ON w.cloud_ID = c.cloud_ID
        WHERE f.DEP_DELAY > 0
        GROUP BY w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER
        ORDER BY delay_count DESC
        LIMIT 10;
    """,
    "weather_impact_on_cancellations": """
        SELECT w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER, COUNT(f.CANCELLED) AS cancellations
        FROM Flight f
        JOIN Weather w ON f.weather_ID = w.weather_ID
        JOIN Cloud c ON w.cloud_ID = c.cloud_ID
        WHERE f.CANCELLED = 2
        GROUP BY w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER
        ORDER BY cancellations DESC
        LIMIT 10;
    """
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Execute queries and print results
    for query_name, query in queries.items():
        print(f"Results for {query_name.replace('_', ' ').title()}:")
        cursor.execute(query)
        results = cursor.fetchall()

        # Print the results
        print(f"{'Temperature':<15}{'Visibility':<15}{'Cloud Cover':<15}{'Count':<10}")
        print("-" * 55)
        for row in results:
            print(f"{row[0]:<15}{row[1]:<15}{row[2]:<15}{row[3]:<10}")
        print("\n")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
