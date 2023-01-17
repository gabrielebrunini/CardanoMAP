//Account Balance
MATCH (s:StakeAddress)
RETURN s.view as stake_address,
CASE
// Need to add reserve
 WHEN (coalesce(s.total_rewards,0)-coalesce(s.total_withdrawals,0)) < 0
 THEN (coalesce(s.utxo,0) + coalesce(s.total_treasuries,0))
 WHEN (coalesce(s.total_rewards,0)-coalesce(s.total_withdrawals,0)) >=0 
 THEN (coalesce(s.utxo,0) + coalesce(s.total_rewards,0) + coalesce(s.total_treasuries,0) - coalesce(s.total_withdrawals,0))
END AS balance
ORDER BY balance DESC