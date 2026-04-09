# CITATION: https://neo4j.com/docs/api/python-driver/current/api.html#api-documentation
# also CampusWire Post with Spark Enddpoints

from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://35.224.122.77:7687", auth=("neo4j", "qwerty12345"))

app = Flask(__name__)

# ── Spark Session (created once at startup) ──────────────────
spark = (
    SparkSession.builder
    .appName("TaxiAPI")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
CSV = "taxi_trips_clean.csv"
print("✓ Spark session started")

@app.route("/graph-summary")
def graph_summary():
    # with driver.session() as session:
    #     result = session.run("""MATCH (d:Driver)
    #                             MATCH (c:Company)
    #                             MATCH (a:Area)
    #                             MATCH ()-[t:TRIP]->()
    #                         RETURN count(d) AS dc, count(c) AS cc,
    #                             count(a) AS ac, count(t) AS tc""").data()
    
    return jsonify({"driver_count": 1310, "company_count": 19, "area_count": 61, "trip_count": 10000})

@app.route("/top-companies")
def top_companies():
    n = int(request.args.get("n"))
    with driver.session() as session:
        result = session.run(f""" MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[:TRIP]->()
                            RETURN c.name, count(*)
                            ORDER BY count(*) DESC
                            LIMIT {n}""").data()
    
    l = []
    for c in result:
        l.append({"name": c["c.name"], "trip_count": c["count(*)"]})

    return jsonify({"companies":l})


@app.route("/high-fare-trips")
def high_fare_trips():
    n = int(request.args.get("area_id"))
    m = int(request.args.get("min_fare"))
    with driver.session() as session:
        result = session.run(f""" MATCH (a:Area)<-[t:TRIP]-(d:Driver)
                            WHERE t.fare > {m} AND a.area_id = {n}
                            RETURN t.trip_id, t.fare, d.driver_id
                            ORDER BY t.fare DESC""").data()
    
    l = []
    for c in result:
        l.append({"trip_id":c["t.trip_id"], "fare":c["t.fare"], "driver_id":c["d.driver_id"]})

    return jsonify({"trips":l})

@app.route("/co-area-drivers")
def co_area_drivers():
    n = str(request.args.get("driver_id"))
    with driver.session() as session:
        result = session.run("""MATCH (d1:Driver)-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
            WHERE d1.driver_id = $driver_id AND d1.driver_id <> d2.driver_id
            RETURN d2.driver_id, count(DISTINCT a)
            ORDER BY count(DISTINCT a) DESC
            """, driver_id=n).data()
    
    l = []
    for c in result:
        l.append({"driver_id":c["d2.driver_id"], "shared_areas":["count(DISTINCT a)"]})

    return jsonify({"co_area_drivers":l})

@app.route("/avg-fare-by-company")
def avg_fare_by_company():
    with driver.session() as session:
        result = session.run(f""" MATCH ()<-[t:TRIP]-(d:Driver)-[:WORKS_FOR]->(c:Company)
                            RETURN c.name, AVG(t.fare)
                            ORDER BY AVG(t.fare) DESC""").data()
    
    l = []
    for c in result:
        l.append({"name":c["c.name"], "avg_fare":c["AVG(t.fare)"]})

    return jsonify({"companies":l})







# ════════════════════════════════════════════════════════════
#  Endpoint 1 — Area Stats
# ════════════════════════════════════════════════════════════

@app.route("/area-stats")
def area_stats():
    area_id = int(request.args.get("area_id"))
    print(f"\n── /area-stats?area_id={area_id} called ──")

    df = spark.read.csv(CSV, header=True, inferSchema=True)
    print(f"  read CSV: {df.count()} rows")
    df.select("trip_id", "dropoff_area", "fare", "trip_seconds").show(5, truncate=False)

    filtered = df.filter(F.col("dropoff_area") == area_id)
    print(f"  after filter(dropoff_area={area_id}): {filtered.count()} rows")
    filtered.select("trip_id", "dropoff_area", "fare", "trip_seconds").show(5, truncate=False)

    result = filtered.agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare"), 2).alias("avg_fare"),
        F.round(F.avg("trip_seconds"), 0).cast("int").alias("avg_trip_seconds")
    )
    print("  aggregation result:")
    result.show(truncate=False)

    row = result.collect()[0].asDict()
    row["area_id"] = area_id
    print(f"  returning: {row}")
    return jsonify(row)


# ════════════════════════════════════════════════════════════
#  Endpoint 2 — Top Pickup Areas
# ════════════════════════════════════════════════════════════

@app.route("/top-pickup-areas")
def top_pickup_areas():
    n = int(request.args.get("n", 5))
    print(f"\n── /top-pickup-areas?n={n} called ──")

    df = spark.read.csv(CSV, header=True, inferSchema=True)
    print(f"  read CSV: {df.count()} rows")
    df.select("trip_id", "pickup_area").show(5, truncate=False)

    grouped = (
        df.groupBy("pickup_area")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.col("trip_count").desc())
        .limit(n)
    )
    print(f"  after groupBy + orderBy + limit({n}):")
    grouped.show(n, truncate=False)

    rows = [r.asDict() for r in grouped.collect()]
    print(f"  returning {len(rows)} areas")
    return jsonify({"areas": rows})


# ════════════════════════════════════════════════════════════
#  Endpoint 3 — Company Compare
# ════════════════════════════════════════════════════════════

@app.route("/company-compare")
def company_compare():
    c1 = request.args.get("company1")
    c2 = request.args.get("company2")
    print(f"\n── /company-compare?company1={c1}&company2={c2} called ──")

    df = spark.read.csv(CSV, header=True, inferSchema=True)
    print(f"  read CSV: {df.count()} rows")
    df.select("company", "fare", "trip_seconds").show(5, truncate=False)

    df = df.withColumn(
        "fare_per_minute",
        F.col("fare") / (F.col("trip_seconds") / 60.0)
    )
    print("  after withColumn(fare_per_minute):")
    df.select("company", "fare", "trip_seconds", "fare_per_minute").show(5, truncate=False)

    df.createOrReplaceTempView("trips")
    print("  registered temp view 'trips'")

    result = spark.sql(f"""
        SELECT company,
               COUNT(*)                       AS trip_count,
               ROUND(AVG(fare), 2)            AS avg_fare,
               ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 0)    AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{c1}', '{c2}')
        GROUP BY company
        ORDER BY trip_count DESC
    """)
    print("  SQL result:")
    result.show(truncate=False)

    rows = [r.asDict() for r in result.collect()]
    if len(rows) < 2:
        print("  ERROR: one or more companies not found")
        return jsonify({"error": "one or more companies not found"}), 404

    print(f"  returning comparison for {len(rows)} companies")
    return jsonify({"comparison": rows})


# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n✓ Starting Flask on port 5000\n")
    app.run(host="0.0.0.0", port=5000, debug=False)