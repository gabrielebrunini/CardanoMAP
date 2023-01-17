//Total Rewards for a Stake Address
MATCH (t:Treasury)-[a1:Recieved_by]->(sa:StakeAddress)
RETURN sa.view AS stake_address, a1.amount AS treasury_reward, NULL AS staking_reward, a1.amount AS total_rewards
UNION
MATCH (r:Rewards)-[a2:Recieved_by]->(sa:StakeAddress)
RETURN sa.view AS stake_address, NULL AS treasury_reward, a2.amount AS staking_reward, a2.amount AS total_rewards
UNION
MATCH (t:Treasury)-[a1:Recieved_by]->(sa:StakeAddress)
MATCH (r:Rewards)-[a2:Recieved_by]->(sa:StakeAddress)
WITH sa, a1, a2, sum(a1.amount + a2.amount) AS total_rewards
RETURN sa.view AS stake_address, a1.amount AS treasury_reward, a2.amount AS staking_reward, total_rewards
ORDER BY total_rewards DESC LIMIT 15