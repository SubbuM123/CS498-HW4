# CampusWire Post with Spark Enddpoints

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ── 1. Start Spark ───────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TaxiTrips")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")  # suppress noisy logs
print("\n✓ Spark session started\n")

# ── 2. Read CSV ──────────────────────────────────────────────
df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

# print("── After read_csv ──")
# print(f"Row count: {df.count()}, Columns: {df.columns}")
# df.show(5, truncate=False)

# ── 3. Add fare_per_minute column ────────────────────────────
df = df.withColumn(
    "fare_per_minute",
    F.col("fare") / (F.col("trip_seconds") / 60.0)
)

print("── After withColumn (fare_per_minute) ──")
# df.select("trip_id", "fare", "trip_seconds", "fare_per_minute").show(5, truncate=False)

# ── 4. Register temp view ────────────────────────────────────
df.createOrReplaceTempView("trips")
# print("── Temp view 'trips' registered ──\n")

# ── 5. Run SQL summary ───────────────────────────────────────
summary_df = spark.sql("""
    SELECT company,
           COUNT(*)                       AS trip_count,
           ROUND(AVG(fare), 2)            AS avg_fare,
           ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
    FROM trips
    GROUP BY company
    ORDER BY trip_count DESC
""")

# print("── Company summary (top 10) ──")
# summary_df.show(10, truncate=False)

# ── 6. Save to JSON ──────────────────────────────────────────
summary_df.write.json("processed_data/", mode="overwrite")
print("✓ Saved to processed_data/")

# ── 7. Verify output ─────────────────────────────────────────
import subprocess
result = subprocess.run(["ls", "-lh", "processed_data/"], capture_output=True, text=True)
print("\n── processed_data/ contents ──")
print(result.stdout)

spark.stop()
print("✓ Done")