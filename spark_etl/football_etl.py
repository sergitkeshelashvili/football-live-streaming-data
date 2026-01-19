import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -------------------------
# Connection Parameters
# -------------------------
DB_USER = "postgres"
DB_PASS = "xxxxxx"
DB_HOST = "127.0.0.1"
MASTER_PORT = 5432

# SOURCE: Default 'postgres' DB where NiFi lands data
SOURCE_DB = "postgres"
SOURCE_URL = f"jdbc:postgresql://{DB_HOST}:{MASTER_PORT}/{SOURCE_DB}"

# TARGET: Analytics DB where we build the Medallion layers
TARGET_DB = "football_db"
TARGET_URL = f"jdbc:postgresql://{DB_HOST}:{MASTER_PORT}/{TARGET_DB}"

JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "quoteIdentifiers": "true"
}


def get_spark():
    """Initializes Spark Session with PostgreSQL Connector."""
    return (SparkSession.builder
            .appName("Football_Medallion_ETL")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate())


def setup_target_db():
    """Creates the analytics database and empty tables on the Master server."""
    # 1. Ensure 'football_db' exists
    conn = psycopg2.connect(host=DB_HOST, port=MASTER_PORT, dbname="postgres", user=DB_USER, password=DB_PASS)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{TARGET_DB}'")
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {TARGET_DB}")
    cur.close()
    conn.close()

    # 2. Setup Schema and Tables inside football_db
    conn = psycopg2.connect(host=DB_HOST, port=MASTER_PORT, dbname=TARGET_DB, user=DB_USER, password=DB_PASS)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS football;")

    # Bronze Table (Raw Data Capture)
    cur.execute("DROP TABLE IF EXISTS football.bronze_football_events;")
    cur.execute("""
                CREATE TABLE football.bronze_football_events
                (
                    event_id   VARCHAR(36),
                    match_id   VARCHAR(50),
                    team       VARCHAR(100),
                    player     VARCHAR(100),
                    event_type VARCHAR(50),
                    timestamp  TIMESTAMP
                );
                """)

    # Silver Table (Cleaned & Unique)
    cur.execute("DROP TABLE IF EXISTS football.silver_football_events;")
    cur.execute("""
                CREATE TABLE football.silver_football_events
                (
                    match_id   VARCHAR(50),
                    team       VARCHAR(100),
                    event_type VARCHAR(50),
                    timestamp  TIMESTAMP
                );
                """)

    # Gold Table (Business Metrics)
    cur.execute("DROP TABLE IF EXISTS football.gold_football_stats;")
    cur.execute("""
                CREATE TABLE football.gold_football_stats
                (
                    team        VARCHAR(100) PRIMARY KEY,
                    total_goals INT,
                    total_fouls INT,
                    total_shots INT
                );
                """)
    cur.close()
    conn.close()
    print(f"--- Target DB '{TARGET_DB}' ready on Master (5432) ---")


def run_etl():
    """Executes the Extraction, Transformation, and Loading logic."""
    spark = get_spark()
    print(f"--- Extracting data from {SOURCE_DB}.public.event_logs ---")

    try:
        # --- 1. EXTRACT ---
        df_source = spark.read.jdbc(SOURCE_URL, "public.event_logs", properties=JDBC_PROPS)

        # --- 2. TRANSFORM BRONZE (Handle Column Names & Missing Timestamps) ---
        # We map 'event_ts' to 'timestamp' and fill empty ones with the current time
        df_bronze = df_source.withColumn(
            "timestamp",
            F.coalesce(F.col("event_ts").cast("timestamp"), F.current_timestamp())
        ).drop("event_ts")

        # --- 3. LOAD BRONZE ---
        print("Loading Bronze Layer...")
        df_bronze.write.jdbc(TARGET_URL, "football.bronze_football_events", mode="append", properties=JDBC_PROPS)

        # --- 4. SILVER (Data Quality) ---
        print("Cleaning Silver Layer...")
        df_silver = df_bronze.filter(F.col("event_type").isNotNull()) \
            .dropDuplicates(["event_id"]) \
            .select("match_id", "team", "event_type", "timestamp")

        df_silver.write.jdbc(TARGET_URL, "football.silver_football_events", mode="overwrite", properties=JDBC_PROPS)

        # --- 5. GOLD (Aggregates) ---
        print("Calculating Gold Stats...")
        df_gold = df_silver.groupBy("team").agg(
            F.count(F.when(F.col("event_type").rlike("(?i)goal"), 1)).alias("total_goals"),
            F.count(F.when(F.col("event_type").rlike("(?i)foul"), 1)).alias("total_fouls"),
            F.count(F.when(F.col("event_type").rlike("(?i)shot"), 1)).alias("total_shots")
        )

        df_gold.write.jdbc(TARGET_URL, "football.gold_football_stats", mode="overwrite", properties=JDBC_PROPS)

        print("--- ETL SUCCESSFUL ---")

    except Exception as e:
        print(f"!!! ETL FAILED: {e} !!!")
    finally:
        spark.stop()


if __name__ == "__main__":
    setup_target_db()
    run_etl()
