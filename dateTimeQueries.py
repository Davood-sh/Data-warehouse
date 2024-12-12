import psycopg2

# Database configuration
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Queries
queries = {
    "delayed_flights_by_day_of_month": """
        SELECT 
            d.DEP_DAY AS day_of_month,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.DEP_DELAY > 0
        GROUP BY d.DEP_DAY
        ORDER BY delayed_flights DESC;
    """,
    "canceled_flights_holidays_vs_non_holidays": """
        SELECT 
            d.Holiday,
            COUNT(f.CANCELLED) AS canceled_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.CANCELLED > 0
        GROUP BY d.Holiday
        ORDER BY canceled_flights DESC;
    """,
    "delayed_flights_holidays_vs_non_holidays": """
        SELECT 
            d.Holiday,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.DEP_DELAY > 0
        GROUP BY d.Holiday
        ORDER BY delayed_flights DESC;
    """,
    "monthly_delays_and_cancellations": """
        SELECT 
            d.DEP_MONTH AS month,
            COUNT(CASE WHEN f.DEP_DELAY > 0 THEN 1 END) AS delayed_flights,
            COUNT(CASE WHEN f.CANCELLED > 0 THEN 1 END) AS canceled_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        GROUP BY d.DEP_MONTH
        ORDER BY month;
    """,
    "peak_days_for_delays": """
        SELECT 
            d.FL_DATE AS flight_date,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.DEP_DELAY > 0
        GROUP BY d.FL_DATE
        ORDER BY delayed_flights DESC
        LIMIT 10;
    """,
    "quarterly_analysis_of_cancellations": """
        SELECT 
            d.DEP_QUARTER AS quarter,
            COUNT(f.CANCELLED) AS canceled_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.CANCELLED > 0
        GROUP BY d.DEP_QUARTER
        ORDER BY canceled_flights DESC;
    """,

    "quarterly_analysis_of_delayed_flights": """
        SELECT 
            d.DEP_QUARTER AS quarter,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.DEP_DELAY > 0
        GROUP BY d.DEP_QUARTER
        ORDER BY delayed_flights DESC;
    """,
    "hourly_distribution_of_delayed_flights": """
        SELECT 
            EXTRACT(HOUR FROM dt.DEP_TIME_ONLY) AS departure_hour,
            COUNT(f.DEP_DELAY) AS delayed_flights
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        WHERE f.DEP_DELAY > 0
        GROUP BY departure_hour
        ORDER BY delayed_flights DESC;
    """,
    "impact_of_holidays_on_delays": """ 
        SELECT 
            d.Holiday,
            AVG(f.DEP_DELAY) AS average_delay_minutes
        FROM Flight f
        JOIN Date_Time dt ON f.DEP_TIME = dt.DEP_TIME
        JOIN Date d ON dt.FL_DATE = d.FL_DATE
        WHERE f.DEP_DELAY > 0
        GROUP BY d.Holiday
        ORDER BY average_delay_minutes DESC;
    """
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Execute queries and display results
    for query_name, query in queries.items():
        print(f"\nResults for {query_name.replace('_', ' ').title()}:\n")
        cursor.execute(query)
        results = cursor.fetchall()

        # Dynamically print headers and rows
        column_names = [desc[0] for desc in cursor.description]
        print("\t".join(column_names))
        print("-" * (len(column_names) * 15))
        for row in results:
            print("\t".join(map(str, row)))

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
