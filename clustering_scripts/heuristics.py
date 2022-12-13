'''CLUSTERING HEURISTICS RULES'''

# Check whether a transaction has a change address by criteria of reuse of input address
# Inputs: set of input addresses, set of output addresses, list of output nodes
# Output: -1 if there is no change address; the change address node if there is one
def check_reused_address(input_addr_set, output_addr_set, outputs):
    change_addr_node = -1
    intersect = input_addr_set.intersection(output_addr_set)
    if len(intersect) == 1:
        change_addr_reused = list(intersect)[0]
        for o in outputs:
            if o["address"] == change_addr_reused:
                change_addr_node = o
                #print("Reuse of address ; TX ID: ", t["id"], change_addr_node)
                break
    return change_addr_node


# Check whether a transaction has a change address by criteria of optimal change.
# Inputs: array of input nodes, array of output nodes
# Output: -1 if there is no change address; the change address node if there is one
def check_optimal_change(inputs, outputs):
    #Optimal change: multiple inputs and exactly 1 output has a lower value than any input
    change_addr_node = -1
    min_input_value = min([inp["value"] for inp in inputs])
    only_one = 0
    for out in outputs:
        if out["value"] < min_input_value:
            change_addr_node = out
            #print("OPTIMAL CHANGE ; TX ID: ", t["id"], change_addr_node)
            only_one += 1
            if only_one > 1:
                break   
    return change_addr_node

# Check whether a transaction contains a change address by criteria of continuing a peeling chain.
# Inputs: (list of transaction outputs, transaction collection, transaction inputs collection)
# Outputs: -1 if there is no change address; the node containing the change address if there is one
def check_peeling_chain(outputs, tx, tx_in):
    change_addr_node=-1
    for out in outputs:
        future_in_records = list(tx_in.find({"tx_out_id":int(out["tx_id"]), "tx_out_index":int(out["index"])}))
        if len(future_in_records) == 0: 
            continue

        for ft in future_in_records:
            future_tx_id = ft["tx_in_id"]
            future_transaction = tx.find({"id":future_tx_id}).next()

            if "inputs" not in future_transaction.keys() or "outputs" not in future_transaction.keys():
                continue

            if len(future_transaction["inputs"]) == 1 and len(future_transaction["outputs"]) == 2:
                change_addr_node = out
                #print("PEELING CHAINS ; TX ID: ", future_tx_id, change_addr_node)
                break
    return change_addr_node         

# Check whether to flag the address as a change address.
# An address is a change address only if it is the only one that satisfies one of the 3 conditions. If there was another address previously flagged as a change address,
# and another address is flagged under a new rule, the change address is set to -1, because more than 1 address satisfy conditions.
# Input: current change address node, node that was determined as pontential change by a rule
# Output: -1 if the conditions determine there shouldnt be a change address; the change address node otherwise
def check_change_address(change_addr_node, potential_change):
	#If the most recent condition found a potential change address
	if potential_change != -1:
		# If the previous conditions did not find a potential change address, make the most recent one the change address
		if change_addr_node == -1:
			change_addr_node = potential_change
		else:
		# If the previous conditions found a potential change address different than the most recent one, the condition for the change address is not fulfilled
		# Set the flag for change address to -1
			if change_addr_node != potential_change:
				change_addr_node = -1
	return change_addr_node
