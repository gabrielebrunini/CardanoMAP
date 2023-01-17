//Set Total Withdrawals for Stake Address
MATCH (s:StakeAddress)-[r:MADE]->(w:Withdrawal)
WITH s, SUM(r.value) as withdrawals
SET s.total_withdrawals = withdrawals