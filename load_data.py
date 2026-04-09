# CITATION : https://neo4j.com/docs/python-manual/6/performance/#batch-data-creation, 
# https://github.com/neo4j-graph-examples/movies/blob/main/scripts/movies.cypher

# numbers = [{"value": random()} for _ in range(10000)]
# driver.execute_query("""
#     UNWIND $numbers AS node
#     MERGE (:Number {value: node.value})
#     """, numbers=numbers,
# )
import pandas as pd
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://35.224.122.77:7687", auth=("neo4j", "qwerty12345"))

df = pd.read_csv("taxi_trips_clean.csv")
d = df.to_dict("records")


driver.execute_query("""
    UNWIND $rows AS r

    WITH r,
            r.pickup_area AS p,
            r.dropoff_area AS d,
            r.fare AS f,
            r.trip_seconds AS t

    MERGE (d1:Driver {driver_id: r.driver_id})
    MERGE (c1:Company {name: r.company})
    MERGE (a1:Area {area_id: p})
    MERGE (a2:Area {area_id: d})

    MERGE (d1)-[:WORKS_FOR]->(c1)

    MERGE (d1)-[t1:TRIP {trip_id: r.trip_id}]->(a2) ON CREATE SET t1.fare = f, t1.trip_seconds = t
""", rows=d)

print("✓ Done")