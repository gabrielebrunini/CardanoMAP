'''Here are stored all functions needed to create specific plot data that describe various properties of the entity network'''
#from db_load import *
import numpy as np


# CORE-PERIPHERY STRUCTURE
# Return the core/periphery structure of the network. The core is defined by the number of nodes included (k_best) and a description of the core nodes (core_description).
# The description is a dictionairy with keys being degree of node and the value the number of nodes with that degree in the network.
def core_periphery(degree_dist):
    degree_list = []
    sum = 0
    for key in sorted(degree_dist.keys(), reverse=True):
        degree_list.extend([int(key)] * degree_dist[key])
        sum += int(key)*degree_dist[key]
    z = 0.5 * sum

    z_best = 10**15
    k_best = -1

    for k in range(0, len(degree_list)):
        v_k = degree_list[k]
        z = z + k - 1- v_k
        if z < z_best:
            z_best = z
            k_best = k

    core_set = degree_list[0:k_best]
    core_description = {}
    for i in set(core_set):
        core_description[str(i)] = core_set.count(i)
    return core_description, k_best

'''ACCOUNT BALANCES'''
# Calculate the balance of the entity. The entity balance is the sum of unspent transaction outputs (utxo). 
# It is calculated by searching whether the utxo shows up in the tx_in collection at the timepoint of calculating.
def calculate_entity_balance(ent_nodes, block_id, tx, tx_in, tx_out):
    balance = 0
    for tx_id, index in ent_nodes:
        tx_in_entry = tx_in.find_one({"tx_out_id":tx_id, "tx_out_index":index})
        # If no entry in the inputs table exists, the output is unspent
        if tx_in_entry is None:
            tx_out_entry = tx_out.find_one({"tx_id":tx_id, "index":index})
            if tx_out_entry is not None:  balance += int(tx_out_entry["value"])
        # If an entry exists, but at a later time point than currently considered, it is also considered unspent
        else:
            tx_entry = tx.find_one({"id":tx_in_entry["tx_in_id"]})
            if tx_entry is not None and tx_entry["block_id"] >= block_id:
                tx_out_entry = tx_out.find_one({"tx_id":tx_id, "index":index})
                if tx_out_entry is not None:  balance +=  int(tx_out_entry["value"])
    return balance


# Function which calculates the Gini index
# Inputs: array with length equal to number of agents; each element in array represents the wealth of the agent
# Outputs: gini index of the system
def gini_index(inp_array):
    array = np.array(inp_array)
    array = array.astype(float)
    array = array.flatten() #all values are treated equally, arrays must be 1d
    if np.amin(array) < 0:
        array -= np.amin(array) #values cannot be negative
    array += 0.0000001 #values cannot be 0
    array = np.sort(array) #values must be sorted
    index = np.arange(1,array.shape[0]+1) #index per array element
    n = array.shape[0] #number of array elements
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array))) #Gini coefficient

'''STRONGEST CONNECTED COMPONENT'''
# Return all strongly connected components in the directed graph
def strongly_connected_components(G):
    preorder={}
    lowlink={}
    scc_found={}
    scc_queue = []
    i=0     

    G_copy = G.copy()
    # Entity
    for node in G:
        if node not in scc_found:
            queue=[node]
            while queue:
                # Take entity from queue
                v=queue[-1]

                if v not in preorder:
                    i=i+1
                    preorder[v]=i

                done=1

                # Get all nodes that node v has outward connections with
                if v not in G_copy.keys(): 
                    G_copy[v]=set()
                    continue

                v_nbrs=G_copy[v]
                for w in v_nbrs:
                    if w not in preorder:
                        queue.append(w)
                        done=0
                        break

                if done==1:
                    lowlink[v]=preorder[v]
                    for w in v_nbrs:
                        if w not in scc_found:
                            if preorder[w]>preorder[v]:
                                lowlink[v]=min([lowlink[v],lowlink[w]])
                            else:
                                lowlink[v]=min([lowlink[v],preorder[w]])
                    queue.pop()
                    if lowlink[v]==preorder[v]:
                        scc_found[v]=True
                        scc=[v]
                        while scc_queue and preorder[scc_queue[-1]]>preorder[v]:
                            k=scc_queue.pop()
                            scc_found[k]=True
                            scc.append(k)
                        yield scc
                    else:
                        scc_queue.append(v)
