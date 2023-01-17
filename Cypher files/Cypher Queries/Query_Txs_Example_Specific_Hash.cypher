//Query Txs Example (Specific Hash)
MATCH (a1:Addresses)-[r1:INPUT_TO]->(t:Transaction)-[r2:OUTPUT_TO]->(a2:Addresses)
WHERE a1.address <> a2.address
AND t.hash = "\x08ce9f082f9b864864d85a55655f015cd70bf0f25e0825cb8a9fbb4f9d4d7d8f"
RETURN DISTINCT a1.address AS Inputs, r1.amount/1000000 AS ADA_Input, a2.address AS Outputs, r2.amount/1000000 AS ADA_Output