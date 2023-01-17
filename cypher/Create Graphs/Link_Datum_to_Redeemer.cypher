//Link Datum to Redeemer
MATCH (d:Datum), (r:Redeemer)
WHERE d.id = r.datum_id
MERGE (r)-[:UNLOCKS]->(d)