//Plutus Transaction Network
MATCH (a1:Addresses)-[:INPUT_TO]->(t:Transaction)-[:OUTPUT_TO]->(a2:Addresses)
MATCH (t)-[:INCLUDES]->(s:Script)
WHERE s.type = "plutus"
RETURN (a1)-[:TRANSACTED_WITH]->(a2)