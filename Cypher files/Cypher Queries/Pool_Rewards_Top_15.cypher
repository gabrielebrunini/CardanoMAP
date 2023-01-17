//Pool Rewards (Top 15)
MATCH (r:Rewards)-[:Recieved_by]->(p:Pool)
WHERE r.pool_id = p.id
RETURN p.view AS pool_id, sum(r.amount)/1000000 AS total_rewards
ORDER BY total_rewards DESC LIMIT 15