import psycopg2

# Database connection settings
DB_CONFIG = {
    "host": "host_ip",
    "port": 5432,
    "dbname": "postgress_dbname",
    "user": "username_db",
    "password": "password_db"
}

def run_validation():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    status = "success"
    records_checked = 0

    try:
        with conn.cursor() as cur:
            # Check raw transactions count
            cur.execute("SELECT COUNT(*) FROM raw_data.transactions;")
            raw_count = cur.fetchone()[0]

            # Check analytics summary count
            cur.execute("SELECT COUNT(*) FROM analytics.hourly_activity_summary;")
            analytics_count = cur.fetchone()[0]

            records_checked = raw_count

            # Basic validation logic
            if raw_count == 0 or analytics_count == 0:
                status = "failed"

            # Log validation result
            cur.execute("""
                INSERT INTO monitoring.pipeline_runs
                (pipeline_stage, status, records_processed)
                VALUES (%s, %s, %s);
            """, ("validation", status, records_checked))

    except Exception as e:
        status = "failed"
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO monitoring.pipeline_runs
                (pipeline_stage, status, records_processed)
                VALUES (%s, %s, %s);
            """, ("validation", status, 0))
        print("Validation error:", e)

    conn.close()

    if status == "success":
        print("Validation passed.")
    else:
        print("Validation failed.")

if __name__ == "__main__":
    run_validation()
