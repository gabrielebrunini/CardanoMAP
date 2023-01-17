//Treasury Rewards
MATCH (t:Treasury)
MATCH (sa:StakeAddress)
WHERE t.addr_id = sa.id
MERGE (t)-[:Recieved_by {amount: t.amount}]->(sa)