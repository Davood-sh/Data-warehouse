import psycopg2

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Query to examine delays by airport and weather conditions, including cloud cover
delay_by_airport_query = """
    SELECT 
        f.ORIGIN AS airport_code,
        s.AIRPORT_NAME,
        s.AIRPORT_CITY, 
        w.TEMPERATURE, 
        w.VISIBILITY, 
        c.CLOUD_COVER, 
        COUNT(f.DEP_DELAY) AS delay_count
    FROM Flight f
    JOIN Weather w ON f.weather_ID = w.weather_ID
    JOIN Cloud c ON w.cloud_ID = c.cloud_ID  -- Joining with Cloud table
    JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE
    WHERE f.DEP_DELAY > 0
      AND w.TEMPERATURE BETWEEN 22.0 AND 28.0
      AND w.VISIBILITY = 10.0
      AND c.CLOUD_COVER IN (3, 4)  -- Example condition: cloud cover is 3 or 4
    GROUP BY f.ORIGIN, s.AIRPORT_NAME, s.AIRPORT_CITY, w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER
    ORDER BY delay_count DESC
    LIMIT 10;
"""

# Query to examine cancellations by airport and weather conditions, including cloud cover
cancellation_by_airport_query = """
    SELECT 
        f.ORIGIN AS airport_code,
        s.AIRPORT_NAME,
        s.AIRPORT_CITY, 
        w.TEMPERATURE, 
        w.VISIBILITY, 
        c.CLOUD_COVER, 
        COUNT(f.CANCELLED) AS cancellation_count
    FROM Flight f
    JOIN Weather w ON f.weather_ID = w.weather_ID
    JOIN Cloud c ON w.cloud_ID = c.cloud_ID  -- Joining with Cloud table
    JOIN Station s ON f.ORIGIN = s.AIRPORT_CODE
    WHERE f.CANCELLED = 2
      AND w.TEMPERATURE BETWEEN 22.0 AND 28.0
      AND w.VISIBILITY = 10.0
      AND c.CLOUD_COVER IN (3, 4)  -- Example condition: cloud cover is 3 or 4
    GROUP BY f.ORIGIN, s.AIRPORT_NAME, s.AIRPORT_CITY, w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER
    ORDER BY cancellation_count DESC
    LIMIT 10;
"""

# Query to examine delays by time of the year (e.g., month) with cloud cover
delay_by_time_query = """
    SELECT 
        EXTRACT(MONTH FROM f.DEP_TIME) AS month,
        w.TEMPERATURE,
        w.VISIBILITY,
        c.CLOUD_COVER,
        COUNT(f.DEP_DELAY) AS delay_count
    FROM Flight f
    JOIN Weather w ON f.weather_ID = w.weather_ID
    JOIN Cloud c ON w.cloud_ID = c.cloud_ID  -- Joining with Cloud table
    WHERE f.DEP_DELAY > 0
      AND w.TEMPERATURE BETWEEN 22.0 AND 28.0
      AND w.VISIBILITY = 10.0
      AND c.CLOUD_COVER IN (3, 4)  -- Example condition: cloud cover is 3 or 4
    GROUP BY month, w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER
    ORDER BY delay_count DESC
    LIMIT 10;
"""

# Query to examine cancellations by time of the year with cloud cover
cancellation_by_time_query = """
    SELECT 
        EXTRACT(MONTH FROM f.DEP_TIME) AS month,
        w.TEMPERATURE,
        w.VISIBILITY,
        c.CLOUD_COVER,
        COUNT(f.CANCELLED) AS cancellation_count
    FROM Flight f
    JOIN Weather w ON f.weather_ID = w.weather_ID
    JOIN Cloud c ON w.cloud_ID = c.cloud_ID  -- Joining with Cloud table
    WHERE f.CANCELLED = 2
      AND w.TEMPERATURE BETWEEN 22.0 AND 28.0
      AND w.VISIBILITY = 10.0
      AND c.CLOUD_COVER IN (3, 4)  -- Example condition: cloud cover is 3 or 4
    GROUP BY month, w.TEMPERATURE, w.VISIBILITY, c.CLOUD_COVER
    ORDER BY cancellation_count DESC
    LIMIT 10;
"""

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Execute query to get delays by airport and weather conditions, including cloud cover
    print("Delays by Airport with Specific Weather Conditions (Including Cloud Cover):")
    cursor.execute(delay_by_airport_query)
    delay_results = cursor.fetchall()

    # Print results for delays by airport
    print(f"{'Airport Code':<15}{'Airport Name':<25}{'City':<20}{'Temperature':<15}{'Visibility':<15}{'Cloud Cover':<15}{'Delay Count':<15}")
    print("-" * 120)
    for row in delay_results:
        print(f"{row[0]:<15}{row[1]:<25}{row[2]:<20}{row[3]:<15}{row[4]:<15}{row[5]:<15}{row[6]:<15}")

    # Execute query to get cancellations by airport and weather conditions, including cloud cover
    print("\nCancellations by Airport with Specific Weather Conditions (Including Cloud Cover):")
    cursor.execute(cancellation_by_airport_query)
    cancellation_results = cursor.fetchall()

    # Print results for cancellations by airport
    print(f"{'Airport Code':<15}{'Airport Name':<25}{'City':<20}{'Temperature':<15}{'Visibility':<15}{'Cloud Cover':<15}{'Cancellation Count':<15}")
    print("-" * 120)
    for row in cancellation_results:
        print(f"{row[0]:<15}{row[1]:<25}{row[2]:<20}{row[3]:<15}{row[4]:<15}{row[5]:<15}{row[6]:<15}")

    # Execute query to get delays by time of the year (e.g., by month) with cloud cover
    print("\nDelays by Time of the Year with Specific Weather Conditions (Including Cloud Cover):")
    cursor.execute(delay_by_time_query)
    delay_time_results = cursor.fetchall()

    # Print results for delays by time of the year
    print(f"{'Month':<10}{'Temperature':<15}{'Visibility':<15}{'Cloud Cover':<15}{'Delay Count':<15}")
    print("-" * 70)
    for row in delay_time_results:
        print(f"{int(row[0]):<10}{row[1]:<15}{row[2]:<15}{row[3]:<15}{row[4]:<15}")

    # Execute query to get cancellations by time of the year with cloud cover
    print("\nCancellations by Time of the Year with Specific Weather Conditions (Including Cloud Cover):")
    cursor.execute(cancellation_by_time_query)
    cancellation_time_results = cursor.fetchall()

    # Print results for cancellations by time of the year
    print(f"{'Month':<10}{'Temperature':<15}{'Visibility':<15}{'Cloud Cover':<15}{'Cancellation Count':<15}")
    print("-" * 70)
    for row in cancellation_time_results:
        print(f"{int(row[0]):<10}{row[1]:<15}{row[2]:<15}{row[3]:<15}{row[4]:<15}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
