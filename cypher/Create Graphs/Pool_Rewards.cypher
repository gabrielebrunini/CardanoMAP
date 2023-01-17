//Pool Rewards
MATCH (r:Rewards), (p:Pool)
WHERE r.pool_id = p.id
MERGE (r)-[:Recieved_by {amount: r.amount}]->(p)