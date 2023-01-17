//Set UTXO to Stake Address
MATCH (ti:TransactionIn), (to:TransactionOut), (s:StakeAddress)
WHERE to.tx_id = ti.tx_out_id 
AND to.index = ti.tx_out_index
AND to.stake_address_id = s.id
SET s.utxo = to.value