//Output Addresses
MATCH (a:Addresses), (t:Transaction), (to:TransactionOut)
WHERE t.id = to.tx_id AND to.address = a.address
MERGE (t)-[:OUTPUT_TO {amount: to.value}]->(a)