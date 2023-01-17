//Link Transactions to Scripts 
MATCH (t:Transaction), (s:Script)
WHERE t.id = s.tx_id
MERGE (t)-[:INCLUDES]->(s)