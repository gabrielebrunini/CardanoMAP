//Latest Block
MATCH (n:Blocks) RETURN MAX(n.block_no)