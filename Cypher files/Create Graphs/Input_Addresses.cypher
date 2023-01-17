//Input Addresses
MATCH (a:Addresses), (ti:TransactionIn), (t:Transaction), (to:TransactionOut)
WHERE ti.tx_out_id = to.tx_id AND t.id = ti.tx_in_id 
AND ti.tx_out_index = to.index AND to.address = a.address
MERGE (a)-[:INPUT_TO {amount: to.value}]->(t)