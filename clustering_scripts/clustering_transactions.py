import itertools
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymongo

from cl_utils import *
import heuristics
from db_load import *
from entity_network import *
#imp.reload(functions_clustering)


#Define Mongo client
client = MongoClient("172.23.149.210", 27017)
ADDRESS_MAX_LENGTH = 120000

# Get collections
block_register = get_block_register(client)
tx, tx_in, tx_out, block = get_bronze_collections(client)
last_index, ent, addr_reg, addr_network_inputs, addr_network_outputs, logs = get_silver_collections(client)

#Determine the start indexes and values
max_block_id = int(block.find_one(sort=[("id", -1)])["id"])

entity_counter = int(last_index.find_one({"collection":"entities"})["last_index"])
current_timestamp = int(last_index.find_one({"collection":"clustering_timestamp_progress"})["timestamp"])
next_timestamp = get_next_timestamp(current_timestamp)
logs_cl = client["cardano_silver"]["clustering_logs"]
# Get processed block ids
block_register_ids = set()
if block_register.estimated_document_count({}) > 0:
    block_register_ids = set([bl["block_id"] for bl in block_register.find({})])

#Iterate over sorted blocks; find the batch of transactions for each block id
while(True):
    try:
        if next_timestamp > datetime.timestamp(datetime.now())*1000000: 
            next_timestamp = datetime.timestamp(datetime.now())*1000000
        block_batch = block.find({ "time" : { "$gt" :  current_timestamp, "$lt" : next_timestamp}}, sort=[("id",1)], batch_size=200)
        block_batch_size = block.count_documents({ "time" : { "$gt" :  current_timestamp, "$lt" : next_timestamp}})

        print("starting block batch", current_timestamp, next_timestamp, block_batch_size)
        logs_cl.insert_one({"message":"starting block", "time":time.time()})
        block_batch_ids = [b["id"] for b in block_batch]

        for current_block_id in block_batch_ids:
            #Check if this block has already been processed
            if block_register.find_one({"block_id":current_block_id}) is not None: continue
 
            batch = [btx["id"] for btx in tx.find({"block_id":current_block_id}, no_cursor_timeout=True)]

            
            #Perform clustering algorithm
            for tind in batch:
                t = tx.find_one({"id":tind})
                # Check if transaction has determined inputs and outputs---------------------------
                if "outputs" not in t.keys():
                    continue

                if "inputs" not in t.keys():
                    #Append to entity and address tables without clustering
                    entity_counter = create_non_clustered_entities(t["outputs"], addr_reg, ent, entity_counter)
                    continue

                # Load inputs and outputs---------------------
                inputs = t["inputs"]
                input_addr_set = set([inp['address'] for inp in inputs])
                outputs = t["outputs"]
                output_addr_set = set([out['address'] for out in outputs])

                # Check heuristics rules for clustering------------------
                change_addr_node = -1
                
                input_len = len(inputs)
                output_len  = len(outputs)

                #Check for Reuse of address
                change_addr_node = heuristics.check_reused_address(input_addr_set, output_addr_set, outputs)

                #Optimal change
                if input_len > 1 and output_len > 1:
                    optimal_change = heuristics.check_optimal_change(inputs, outputs)
                    change_addr_node = heuristics.check_change_address(change_addr_node, optimal_change)

                #Check for peeling chains
                if input_len == 1 and output_len == 2:
                    peeling_chain = heuristics.check_peeling_chain(outputs, tx, tx_in)
                    change_addr_node = heuristics.check_change_address(change_addr_node, peeling_chain)

                # Divide inputs and outputs for clustering-----------------------------

                # Make 2 groups: the nodes in cluster list will all be a part of the same entity; 
                # the not_cluster nodes will all be independent entities
                cluster, not_cluster = get_cluster_sets(inputs, outputs, change_addr_node)

                # Create entity of a large cluster------------------------------
                if len(cluster) > 0:
                    entity_counter = create_clustered_entity(cluster, addr_reg, ent, entity_counter)

                # Create independent entities that dont belong to the same cluster--------------------
                entity_counter = create_non_clustered_entities(not_cluster, addr_reg, ent, entity_counter)
                
                # Add neighboring nodes to the lists in address network--------------------  
                add_to_address_network(inputs, target_col="address_network_outputs", target_field = "outputs",\
                    addr_set_in=output_addr_set, max_length=ADDRESS_MAX_LENGTH)

                add_to_address_network(outputs, target_col="addr_network_inputs", target_field = "inputs",\
                    addr_set_in=input_addr_set, max_length=ADDRESS_MAX_LENGTH)

            #Add to the register
            insert_logs(block_register, last_index, current_block_id, entity_counter)
            block_register_ids.add(current_block_id)

    
    except pymongo.errors.AutoReconnect:
        print("!ERROR! auto reconnect error")
        time.sleep(10)
        client = MongoClient("172.23.149.210", 27017)
        tx, tx_in, tx_out, block = get_bronze_collections(client)
        last_index, ent, addr_reg, addr_network_inputs, addr_network_outputs, logs = get_silver_collections(client)
        continue
    logs_cl.insert_one({"message":"ended block", "time":time.time()})

    # Perform analysis for plots after every month of data ------------------
    print("start clustering ", datetime.fromtimestamp(next_timestamp/1000000), datetime.now())
    calculate_entity_network(client, next_timestamp, block_id=current_block_id, do_addr_no=True, do_deg_dist=True, do_core=True, do_balances=True, do_scc=True)
    
    # Update results and break condition-----------------------------------
    current_timestamp = next_timestamp

    last_index.update_one({"collection":"clustering_timestamp_progress"}, {"$set":{"timestamp":next_timestamp}}, upsert=True)

    next_timestamp  = get_next_timestamp(next_timestamp)
    
    
    if len(block_register_ids) >= max_block_id:
        print("Stopping loop, all blocks processed")
        break
        

    