//Set Total Rewards For Stake Address
MATCH (rw:Rewards)-[r:Recieved_by]->(s:StakeAddress)
WITH s, SUM(r.amount) as rewards
SET s.total_rewards = rewards