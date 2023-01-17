//Set Total Treasury Rewards for Stake Address
MATCH (t:Treasury)-[r:Recieved_by]->(s:StakeAddress)
WITH s, SUM(r.amount) as treasuries
SET s.total_treasuries = treasuries