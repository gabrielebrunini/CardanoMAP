//Transaction Network Related to Scripts
MATCH (a1:Addresses)-[:INPUT_TO]->(t:Transaction)-[:OUTPUT_TO]->(a2:Addresses)
MATCH (t)-[:INCLUDES]->(s:Script)
RETURN (a1)-[:TRANSACTED_WITH]->(a2)