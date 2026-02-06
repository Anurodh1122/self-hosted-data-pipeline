import random
import psycopg2
from datetime import datetime, timedelta

# Database connection settings
DB_CONFIG = {
    "host": "192.168.178.51",
    "port": 5432,
    "dbname": "MyDB",
    "user": "root",
    "password": "root"
}

EVENT_TYPES = ["purchase", "refund", "subscription"]
REGIONS = ["EU", "US", "ASIA"]


def generate_transaction():
    return (
        random.randint(1, 50),
        datetime.now() - timedelta(minutes=random.randint(0, 300)),
        random.choice(EVENT_TYPES),
        round(random.uniform(10, 500), 2),
        random.choice(REGIONS)
    )
    
def insert_transactions(conn, count=10):
    with conn.cursor() as cur:
        for _ in range(count):
            txn = generate_transaction()
            cur.execute("""
                INSERT INTO raw_data.transactions
                (account_id, event_time, event_type, amount, region)
                VALUES (%s, %s, %s, %s, %s);
            """, txn)

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    insert_transactions(conn, count=10)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO monitoring.pipeline_runs
            (pipeline_stage, status, records_processed)
            VALUES (%s, %s, %s);
        """, ("ingestion", "success", 10))


    conn.close()
    print("Inserted 10 generated transactions.")

if __name__ == "__main__":
    main()