import psycopg2

# Database connection settings
DB_CONFIG = {
    "host": "192.168.178.51",
    "port": 5432,
    "dbname": "MyDB",
    "user": "root",
    "password": "root"
}

def run_transformation():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        # Clear old analytics data
        cur.execute("""
            TRUNCATE TABLE analytics.hourly_activity_summary;
        """)

        # Insert hourly aggregated data
        cur.execute("""
            INSERT INTO analytics.hourly_activity_summary
            (activity_hour, total_transactions, total_amount)
            SELECT
                DATE_TRUNC('hour', event_time) AS activity_hour,
                COUNT(*) AS total_transactions,
                SUM(amount) AS total_amount
            FROM raw_data.transactions
            GROUP BY activity_hour;
        """)
        
        # Log transformation
        cur.execute("""
            INSERT INTO monitoring.pipeline_runs
            (pipeline_stage, status, records_processed)
            VALUES (%s, %s, %s);
        """, ("transformation", "success", 0))


    conn.close()
    print("Transformation completed.")

if __name__ == "__main__":
    run_transformation()
