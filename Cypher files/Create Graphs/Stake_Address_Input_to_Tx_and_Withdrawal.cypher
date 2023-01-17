//Stake Address Input to Tx and Withdrawal
MATCH (w:Withdrawal),(t:Transaction),(to:TransactionOut),(a)-[:Matched_with]->(s)
WHERE w.tx_id = t.id 
AND w.addr_id = s.id 
AND to.address = a.address
AND to.tx_id = t.id
MERGE (s)-[:INPUT_TO {value: w.amount}]->(t)
MERGE (s)-[r:MADE {value: w.amount}]->(w)