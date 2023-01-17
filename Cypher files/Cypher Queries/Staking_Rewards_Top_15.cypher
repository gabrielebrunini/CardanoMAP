//Staking Rewards (Top 15)
MATCH (r:Rewards)-[:Recieved_by]->(s:StakeAddress)
RETURN s.view as stake_address, SUM(r.amount)/1000000 as total_rewards
ORDER BY total_rewards DESC LIMIT 15