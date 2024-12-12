import psycopg2

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Query to find carriers that use in average the aircrafts with most delayed and canceled flights
query = """
    WITH Most_Delayed_Canceled_Aircraft AS (
        SELECT 
            f.TAIL_NUM,
            COUNT(CASE WHEN f.DEP_DELAY > 0 THEN 1 END) AS total_delays,
            COUNT(CASE WHEN f.CANCELLED > 0 THEN 1 END) AS total_cancellations,
            COUNT(*) AS total_flights
        FROM Flight f
        GROUP BY f.TAIL_NUM
         ORDER BY COUNT(CASE WHEN f.DEP_DELAY > 0 THEN 1 END) + COUNT(CASE WHEN f.CANCELLED > 0 THEN 1 END) DESC
        LIMIT 1000 -- We want to get top 1000 aircraft for calculation
    ),
    Carrier_Usage AS (
        SELECT 
            c.DESCRIPTION AS carrier_name,
            f.TAIL_NUM,
            AVG(f.DEP_DELAY) AS avg_delay,
            AVG(f.CANCELLED::INT) AS avg_cancellation_rate
        FROM Most_Delayed_Canceled_Aircraft mdc
        JOIN Flight f ON mdc.TAIL_NUM = f.TAIL_NUM
        JOIN Carrier c ON f.OP_UNIQUE_CARRIER = c.CODE
        GROUP BY c.DESCRIPTION, f.TAIL_NUM
    ),
    Carrier_Average AS (
        SELECT 
            carrier_name,
            COUNT(DISTINCT TAIL_NUM) AS aircraft_count,
            AVG(avg_delay) AS avg_delay,
            AVG(avg_cancellation_rate) AS avg_cancellation_rate
        FROM Carrier_Usage
        GROUP BY carrier_name
    )
    SELECT 
        carrier_name,
        aircraft_count,
        avg_delay,
        avg_cancellation_rate
    FROM Carrier_Average
    ORDER BY avg_delay DESC, avg_cancellation_rate DESC
    LIMIT 10;
"""

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Execute the query
    print("\nCarriers Using Aircraft with Most Delayed and Canceled Flights (Top 10):")
    cursor.execute(query)
    results = cursor.fetchall()

    # Print the results in a tabular format
    headers = ["Carrier Name", "Aircraft Count", "Average Delay", "Avg Cancellation Rate"]
    print(" | ".join(f"{header:<25}" for header in headers))
    print("-" * (25 * len(headers)))

    for row in results:
        print(" | ".join(f"{str(value):<25}" for value in row))

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
