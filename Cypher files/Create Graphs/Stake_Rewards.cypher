//Stake Rewards
MATCH (r:Rewards)
MATCH (sa:StakeAddress)
WHERE r.addr_id = sa.id
MERGE (r)-[:Recieved_by {amount: r.amount}]->(sa)