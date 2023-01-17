// Highest degree Addresses (Top 15)
MATCH (a1)-[:TRANSACTED_WITH]->(a2), (a3)-[:TRANSACTED_WITH]->(a1)
RETURN a1.address as address, size(collect(a2)) + size(collect(a3)) as degree
ORDER BY degree DESC LIMIT 15