//Transactions (Tokens) Among Addresses
MATCH (tkn1:Asset)-[]-(mo1:MultiAssetOut)-[]-(to1:TransactionOut)-[]-(ti:TransactionIn)-[]-(t:Transaction)-[]-(to2:TransactionOut)-[]-(mo2:MultiAssetOut)-[]-(tkn2:Asset)
MATCH (a1:Addresses)-[r1:INPUT_TO]->(t)-[r2:OUTPUT_TO]->(a2:Addresses) 
WHERE to1.address = a1.address AND to2.address = a2.address 
AND a1.address <> a2.address AND tkn1.name = tkn2.name
MERGE (a1)-[r:TRANSACTED_WITH {hash:t.hash, ADA_Input: r1.amount/1000000, ADA_Output: r2.amount/1000000}]->(a2)
WITH r, mo1, substring(tkn1.name,2) as token
CALL apoc.create.setRelProperty(r, token, mo1["quantity"]) YIELD rel
RETURN rel