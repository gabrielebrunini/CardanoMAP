//Highest Outdegree Addresses (Top 15)
MATCH (a1)-[r:TRANSACTED_WITH]->(a2)
RETURN a1.address, size(collect(a2)) as out_degree
ORDER BY out_degree DESC LIMIT 15