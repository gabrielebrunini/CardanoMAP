//Link Datum to Transaction
MATCH (t:Transaction), (d:Datum)
WHERE t.id = d.tx_id
MERGE (d)-[:ASSOCIATED_WITH]->(t)