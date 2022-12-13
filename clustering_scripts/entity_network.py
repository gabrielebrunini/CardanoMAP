from db_load import *
from ent_net_properties import *
import time
from cl_utils import *
import pymongo
import multiprocessing
from functools import partial


def divide_to_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Get all entities that are in contact with the current entity
# Input: list of addresses that form the current entity; id of the current entity, it should be excluded from input and output sets
# Output: set of entities that are inputs to current entity, set of entities that are outputs to current entity
def get_entity_neighbours(client, addresses, current_id):
    input_ents = set()
    output_ents = set()
    _, _, addr_reg, addr_network_inputs, addr_network_outputs, _ = get_silver_collections(client)
    # For all addresses within an entity, find their neighbours
    for adr in addresses:
        # Get address inputs; there can be multiple documents for the same address in the collection
        addr_in = [a for a in addr_network_inputs.find({"address":adr})]
        for a in addr_in:
            # For every entry loop over all neighbours and add the entity the neighbour is part of
            for ain in a["inputs"]:
                e = addr_reg.find_one({"address": ain})
                if e is not None:
                    input_ents.add(e["entity_id"])

        addr_out = [a for a in addr_network_outputs.find({"address":adr})]
        for a in addr_out:
            for aout in a["outputs"]:
                e = addr_reg.find_one({"address": aout}) 
                if e is not None:
                    output_ents.add(e["entity_id"])

    input_ents.discard(current_id)
    output_ents.discard(current_id)

    return input_ents, output_ents


def helper_func(chunk, do_core=True, do_scc=True, do_deg_dist=True, do_balances=True, do_addr_no=True):
    client = get_client()
    helper_entity_graph, helper_deg_dist, helper_wealth, helper_adr_dist = get_helper_collections(client)
    _, ent, _, _, _, _ = get_silver_collections(client)
    print(chunk)
    for ind in chunk:
        e = ent.find_one({"id":ind})
        if e is None: continue
        
        if do_deg_dist or do_core or do_scc:
            input_entities, output_entities = get_entity_neighbours(client, e["addresses"], e["id"]) 
            helper_entity_graph.insert_one({"id":e["id"], "out": list(output_entities)})

        if do_deg_dist:
            #Update dictionaries that store degree distributions
            helper_deg_dist.insert_one({"in":len(input_entities), "out":len(output_entities), "total":len(input_entities.union(output_entities))})

        if do_balances:
            tx, tx_in, tx_out, _ = get_bronze_collections(client)
            entity_balance = calculate_entity_balance(e["nodes"], block_id2, tx=tx, tx_in=tx_in, tx_out=tx_out)
            helper_wealth.insert_one({"balance":entity_balance})

        if do_addr_no:
            #Keep track of the number of addresses in entity
            helper_adr_dist.insert_one({"adr_no":len(e["addresses"])})


def calculate_entity_network(client, timest, block_id, do_addr_no=True, do_deg_dist=True, do_core=True, do_balances=True, do_scc=True):
    start_time = time.time()
    _, ent, addr_reg, _, _, logs = get_silver_collections(client)
    # Get number of entries in entity and address register collections
    logs_ent = client["cardano_silver"]["entity_logs"]

    logs_ent.insert_one({"time":time.time(), "message":"Started entity loop"})
    ent_count = ent.estimated_document_count({})
    addr_count = addr_reg.estimated_document_count({})

    loop_time = gini_time = core_time = scc_time = 0

    #Store a directed graph in dictionary form; nodes are keys and values are lists of output connections from particular nodes
    print("start entity loop")
    start_loop = time.time()

    max_entity_id = int(ent.find_one(sort=[("id", -1)])["id"])
    min_entity_id = int(ent.find_one(sort=[("id", 1)])["id"])
    
    while(True):
        try:
            global block_id2 
            block_id2 = block_id
            clean_helper_collections(client)
            
            logs_ent.insert_one({"time":time.time(), "message":"Starting multiprocessing loop"})
            print("starting")
            with multiprocessing.Pool() as pool:
                calculate_partial  = partial(helper_func) #partial function creation to allow input to be sent to the calculate function
                entity_ids = [i for i in range(min_entity_id, max_entity_id, 1)]
                pool.map(calculate_partial, list(divide_to_chunks(entity_ids,1000)))   #creates chunks of 1000 document id's
            
            logs_ent.insert_one({"time":time.time(), "message":"Ended multiprocessing loop"})
            break
            
        except pymongo.errors.AutoReconnect:
            print("auto reconnect error in entity")
            time.sleep(10)
            if time.time()-start_loop > 15*60: break
            continue  

    loop_time = time.time()-start_loop
    print(loop_time)
    logs_ent.insert_one({"time":time.time(), "message":"Finished entity loop"})

    ad_vs_ent, ad_vs_ent_dist, degree_dist, core, balances, scc= get_gold_collections(client)
    helper_entity_graph, helper_deg_dist, helper_wealth, helper_adr_dist = get_helper_collections(client)

    # Store the ADDRESS DISTRIBUTION and ADDRESSES VS NUMBER OF ENTITIES
    if do_addr_no:
        print("address vs entity update")
        address_distribution = summarize_collection(helper_adr_dist, "adr_dist")
        ad_vs_ent.update_one({"timestamp": timest/1000000}, {"$set":{"timestamp": timest/1000000, "no_ent": ent_count, "no_addr": addr_count}}, upsert=True)
        ad_vs_ent_dist.update_one({"timestamp": timest/1000000}, {"$set":{"timestamp": timest/1000000, "distribution": address_distribution}}, upsert=True) 


    # Store the DEGREE DISTRIBUTION
    if do_deg_dist:
        print("degree update")
        degree_distribution_in, degree_distribution_out, degree_distribution_total = summarize_collection(helper_deg_dist, "deg_dist")
        degree_dist.update_one({"timestamp": timest/1000000}, {"$set":{"timestamp": timest/1000000,"ditribution_in": degree_distribution_in, \
                           "ditribution_out":degree_distribution_out, "ditribution_total":degree_distribution_total}}, upsert=True) 

    logs_ent.insert_one({"time":time.time(), "message":"Started SCC"})

    # Calculate and store the longest strongly linked component
    if do_scc:
        print("scc update")
        scc_start = time.time()
        entity_graph = summarize_collection(helper_entity_graph, "entity_graph")
        sccs = strongly_connected_components(entity_graph)
        max_scc = max([len(j) for j in sccs])
        scc.update_one({"timestamp": timest/1000000}, {"$set":{"scc_size" : max_scc}}, upsert=True)
        scc_time = time.time()-scc_start

    logs_ent.insert_one({"time":time.time(), "message":"Finished SCC"})

    # Calculate and store the CORE/PERIPHERY STRUCTURE
    if do_core:
        print("core update")
        core_start = time.time()
        _, _, degree_distribution_total = summarize_collection(helper_deg_dist, "deg_dist")
        core_description, core_length = core_periphery(degree_dist=degree_distribution_total)
        core.update_one({"timestamp": timest/1000000}, {"$set":{"core_description":core_description, "total_entities":ent_count, "core_length":core_length}}, upsert=True)
        core_time = time.time()-core_start

    # Calculate and store WEALTH DISTRIBUTION
    if do_balances:
        print("balance update")
        gini_start = time.time()
        wealth_by_entity, wealth_by_entity_no_zeros = summarize_collection(helper_wealth, "balance")
        gini = gini_index(wealth_by_entity)
        gini_no_zeros = gini_index(wealth_by_entity_no_zeros)
        average_wealth = sum(wealth_by_entity)/len(wealth_by_entity)
        balances.update_one({"timestamp": timest/1000000}, {"$set":{"gini_index":gini, "gini_index_no_zeros":gini_no_zeros,"average_wealth":average_wealth}}, upsert=True)
        gini_time = time.time()-gini_start

    logs_ent.insert_one({"time":time.time(), "message":"Finished everything"})

    end_time = time.time()
    logs.insert_one({"message": "entity network processing ", "timestamp": timest/1000000, "time_started":start_time, \
        "time_ended":end_time, "processing_time":(end_time-start_time), "entity_loop": loop_time,  "scc":scc_time, "gini_index":gini_time, "core_periphery":core_time})


