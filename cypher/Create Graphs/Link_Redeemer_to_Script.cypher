//Link Redeemer to Script
MATCH (s:Script), (r:Redeemer) 
WHERE s.hash = r.script_hash
MERGE (r)-[l:ASSOCIATED_WITH]->(s)