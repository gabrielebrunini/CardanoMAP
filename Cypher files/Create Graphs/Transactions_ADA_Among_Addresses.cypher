//Transactions (ADA) Among Addresses
MATCH (a1:Addresses)-[r1:INPUT_TO]->(t:Transaction)-[r2:OUTPUT_TO]->(a2:Addresses)
WHERE a1.address <> a2.address
MERGE (a1)-[:TRANSACTED_WITH {hash: t.hash, ADA_Input: r1.amount/1000000, ADA_Output: r2.amount/1000000}]->(a2)