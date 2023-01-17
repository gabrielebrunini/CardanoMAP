//Stake-Payment Address Transactions
MATCH (s:StakeAddress)-[r1:INPUT_TO]->(t:Transaction)-[r2:OUTPUT_TO]->(a)
MERGE (s)-[:TRANSACTED_WITH {hash: t.hash, ADA_Input: r1.amount, ADA_Output: r2.amount}]->(a)