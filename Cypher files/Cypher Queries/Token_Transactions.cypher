//Token Transactions
MATCH (tkn1:Asset)-[]-(mo1:MultiAssetOut)-[]-(to1:TransactionOut)-[]-(ti:TransactionIn)-[]-(t:Transaction)-[]-(to2:TransactionOut)-[]-(mo2:MultiAssetOut)-[]-(tkn2:Asset)
MATCH (a1:Addresses)-[:INPUT_TO]->(t)-[:OUTPUT_TO]->(a2:Addresses)
MATCH p=(a1)-[r:TRANSACTED_WITH]->(a2)
WHERE to1.address = a1.address AND to2.address = a2.address
AND a1.address <> a2.address AND tkn1.name = tkn2.name AND size(keys(r)) > 3
RETURN p