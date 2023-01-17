//Latest Epoch
MATCH (n:Blocks) RETURN MAX(n.epoch_no)