//Link Shelly Address to Stake Address
MATCH (a:Addresses)<-[:Matched_with]->(sa:StakeAddress)
RETURN a.address, sa.view