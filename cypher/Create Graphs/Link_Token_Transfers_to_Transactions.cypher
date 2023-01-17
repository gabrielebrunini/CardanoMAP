//Link Token Transfers to Transactions 
MATCH (m:MultiAssetOut), (a:Asset), (t:Transaction), (to:TransactionOut)
WHERE m.ident = a.id AND m.tx_out_id = to.id AND t.id = to.tx_id
MERGE (a)-[:SENT_IN {quantity: m.quantity}]->(t)