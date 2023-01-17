//Transaction Network Table
MATCH (a1:Addresses)-[r:TRANSACTED_WITH]->(a2:Addresses)
WHERE a1.address <> a2.address
RETURN a1.address AS from, a2.address AS to, r.amount AS amount
UNION
MATCH (s:StakeAddress)-[r2:TRANSACTED_WITH]->(a:Addresses)
RETURN s.view AS from, a.address AS to, r2.amount AS amount