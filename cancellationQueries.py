import psycopg2

# Define database connection details
db_config = {
    "dbname": "flight_analysis",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",  # Default PostgreSQL port
}

# Query to find the total number of cancellations by reason
query = """
    SELECT c.CANCELLATION_REASON, COUNT(f.CANCELLED) AS total_cancellations
    FROM Flight f
    INNER JOIN Cancellation c ON f.CANCELLED = c.STATUS
    GROUP BY c.CANCELLATION_REASON
    ORDER BY total_cancellations DESC;
"""

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Execute the query
    print("Total Number of Cancellations by Reason:")
    cursor.execute(query)
    results = cursor.fetchall()

    # Print the results
    print(f"{'Cancellation Reason':<30}{'Total Cancellations':<20}")
    print("-" * 50)
    for row in results:
        print(f"{row[0]:<30}{row[1]:<20}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
