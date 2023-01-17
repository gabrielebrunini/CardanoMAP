//Linking Token Minting to Transaction
MATCH (m:MultiAssetMint), (a:Asset), (t:Transaction)
WHERE m.ident = a.id AND m.tx_id = t.id
MERGE (a)-[:MINTED_IN {quantity: m.quantity}]->(t)