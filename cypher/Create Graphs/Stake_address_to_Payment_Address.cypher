//Stake address to Payment Address
MATCH (s:StakeAddress)-[r1:INPUT_TO]->(t:Transaction)-[r2:OUTPUT_TO]->(a:Addresses)
MERGE (s)-[:TRANSACTED_WITH {hash: t.hash, ADA_Input: r1.amount/1000000, ADA_Output: r2.amount/1000000}]->(a)