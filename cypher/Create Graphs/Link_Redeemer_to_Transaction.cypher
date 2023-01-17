//Link Redeemer to Transaction
MATCH (t:Transaction), (r:Redeemer)
WHERE t.id = r.tx_id
MERGE (r)-[:ASSOCIATED_WITH]->(t)