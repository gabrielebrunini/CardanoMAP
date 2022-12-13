from datetime import datetime
from dateutil.relativedelta import relativedelta
import itertools
import multiprocessing
from db_load import *
# Given: (dictionary, key)
# In given dictionary, increase the value under key by 1 if the key already exists in dictionary.
# If it does not exist, insert value 1 under key.
def update_dict_value(dictionary, key):
    if key not in dictionary.keys():
        dictionary[key] = 1
    else:
        dictionary[key] += 1

# Get the timestamp that is one month ahead of the input timestamp
# Input: timestamp expressed in miliseconds
# Output: timestamp that is one month ahead of the input timestamp, expressed in miliseconds
def get_next_timestamp(start_timestamp):
    start_time = start_timestamp  / 1000000
    start_time = datetime.fromtimestamp(start_time)
    date_after_month = start_time + relativedelta(months=4)
    timestamp_after_month = int(datetime.timestamp(date_after_month) * 1000000)
    return timestamp_after_month

def insert_logs(block_register, last_index, current_block_id, entity_counter):
    block_register.insert_one({"block_id":current_block_id}) 

    last_index.update_one({"collection":"block_clustering"}, {"$set": {"last_index": current_block_id}}, upsert=True)
    last_index.update_one({"collection":"entities"}, {"$set": {"last_index": entity_counter}}, upsert=True) 
    last_index.update_one({"progress":"block_clustering"}, {"$set": {"last_index": current_block_id, "time":datetime.now()}}, upsert=True)

# Add to address_network_inputs or address_network_outputs collections.
# Inputs: array of input or output nodes; target collection; field that should be updated (inputs or outputs)
# the set of input or output addresses
def add_to_address_network(oarray, target_col, target_field,addr_set_in, max_length):
    #args = [(o, target_col, target_field, addr_set_in, max_length) for o in oarray]
    #with multiprocessing.Pool() as pool:
    #    pool.starmap(helper_add_network, args)
    for o in oarray:
        target_collection = get_client()["cardano_silver"][target_col]
        my_query = {"address":o["address"], "size" : {"$lt": max_length}}
        result1 = target_collection.find_one(my_query)

        addr_set = set()
        if result1 is not None and target_field in result1.keys():
            addr_set = set(result1[target_field])
        addr_set = list(addr_set.union(addr_set_in))

        update_rule = {"$set":{target_field:addr_set, "size":len(addr_set)}}
        target_collection.update_one(my_query, update_rule, upsert = True) 
        
def helper_add_network(o, target_col, target_field, addr_set_in, max_length):
    target_collection = get_client()["cardano_silver"][target_col]
    
    my_query = {"address":o["address"], "size" : {"$lt": max_length}}
    result1 = target_collection.find_one(my_query)

    addr_set = set()
    if result1 is not None and target_field in result1.keys():
        addr_set = set(result1[target_field])
    addr_set = list(addr_set.union(addr_set_in))

    update_rule = {"$set":{target_field:addr_set, "size":len(addr_set)}}
    target_collection.update_one(my_query, update_rule, upsert = True)

def create_non_clustered_entities(node_array, addr_reg, ent, entity_counter):
    for no in node_array:
        addr_file = addr_reg.find_one({"address":no["address"]})

        #If this address is not in the address register, add it; else leave it alone
        if addr_file is None:
            entity_counter += 1
            addr_reg.insert_one({"address": no["address"], "entity_id": entity_counter})
            ent_node = [no["tx_id"], no["index"]]
            result = {"id": entity_counter, "nodes": [ent_node], "addresses": [no["address"]]}
            ent.insert_one(result)

    return entity_counter

def create_clustered_entity(node_array, addr_reg, ent, entity_counter):
    new_nodes = []
    new_addresses = []
    entity_counter += 1

    # Iterate over all nodes that form a cluster; if they are already registered, update their entity id and if they are not add them to the register
    for node in node_array:
        addr_file = addr_reg.find_one({"address":node["address"]})

        one_node = [node["tx_id"], node["index"]]
        new_nodes.append(one_node)
        new_addresses.append(node["address"])
        
        #If the address is already in the register, find the entity that contains it and add the nodes of the entity
        if addr_file is not None:
            entity_id = addr_file["entity_id"]
            entity = ent.find_one({"id":entity_id})
            
            if entity is not None:
                new_nodes.extend(entity["nodes"])
                new_addresses.extend(entity["addresses"])

                #Delete the entity that previously had this address and update the address register
                ent.delete_one({"id":entity_id})
        
        addr_reg.update_one({"address": node["address"]}, {"$set": {"entity_id": entity_counter}}, upsert=True)

    new_nodes.sort()
    new_nodes = list(k for k,_ in itertools.groupby(new_nodes))
    new_entity = {
        "id":entity_counter,
        "nodes":new_nodes,
        "addresses":list(set(new_addresses))
    }
    ent.insert_one(new_entity)
    return entity_counter

def get_cluster_sets(inputs, outputs, change_addr_node):
    cluster = [] 
    not_cluster = []

    # All outputs are independent entries, except the change address which will be removed if it exists
    not_cluster.extend(outputs)

    #If there is a change address, combine the change address with all of the inputs
    if change_addr_node != -1:
        cluster.extend(inputs)
        cluster.append(change_addr_node)
        not_cluster.remove(change_addr_node)

    #If there isn't a change address, cluster only if there are multiple inputs
    else:
        if len(inputs) > 1:
            cluster.extend(inputs)
    
    return cluster, not_cluster



def summarize_to_dict(arr):
    keys = set(arr)
    result = {}
    for k in keys:
        result[str(k)] = arr.count(k)
    return result


def summarize_collection(coll, mode):
    if mode == "entity_graph":
        result = {}
        for d in coll.find({}):
            result[str(d["id"])] = set(map(str, d["out"]))
        return result

    elif mode == "deg_dist":
        dist_in = dist_out = dist_total = []
        for d in coll.find({}):
            dist_in.append(d["in"])
            dist_out.append(d["out"])
            dist_total.append(d["total"])
        return summarize_to_dict(dist_in), summarize_to_dict(dist_out), summarize_to_dict(dist_total)
    
    elif mode == "balance":
        result1 = result2 = []
        for b in coll.find({}):
            result1.append(int(b["balance"]))
            if int(b["balance"]) > 0:
                result2.append(int(b["balance"]))
        return result1, result2
    else:
        result = [a["addr_no"] for a in coll.find({})]
        return summarize_collection(result)
