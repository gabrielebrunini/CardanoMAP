'''LOAD THE DATABASE AND COLLECTIONS'''
from pymongo import MongoClient

def get_block_register(client):
    db = client["cardano_silver"]
    return db["block_register"]

def get_bronze_collections(client):
    db = client['cardano_bronze']

    #Collections of input blockchain data
    tx = db["node.public.tx"]
    tx_in = db["node.public.tx_in"]
    tx_out = db["node.public.tx_out"]
    block = db["node.public.block"]
    return tx, tx_in, tx_out, block

# Collections of data where intermediate results will be stored
def get_silver_collections(client):
    db_silver = client["cardano_silver"]

    last_index = db_silver["last_indexes"]
    ent = db_silver["entities"]
    addr_reg = db_silver["address_register"]
    addr_network_inputs = db_silver["address_network_inputs"]
    addr_network_outputs = db_silver["address_network_outputs"]
    logs = db_silver["logs"]

    return last_index, ent, addr_reg, addr_network_inputs, addr_network_outputs, logs


def get_gold_collections(client):
    # Collections of data with output results for plot creation
    plot_db = client["cardano_gold"]
    ad_vs_ent = plot_db["ADvsENT"]
    ad_vs_ent_dist = plot_db["ADvsENT_dist"]
    degree_dist = plot_db["DEG_dist"]
    core = plot_db["core"]
    balances = plot_db["balances"]
    scc = plot_db["SCC"]
    return ad_vs_ent, ad_vs_ent_dist, degree_dist, core, balances, scc

def get_helper_collections(client):
    main_db = client["cardano_silver"]
    helper_entity_graph = main_db["helper_entity_graph"]
    helper_deg_dist = main_db["helper_dist_in"]
    helper_wealth = main_db["helper_wealth"]
    helper_adr_dist = main_db["helper_adr_dist"]
    return helper_entity_graph, helper_deg_dist, helper_wealth, helper_adr_dist

def clean_helper_collections(client):
    helper_entity_graph, helper_deg_dist, helper_wealth, helper_adr_dist = get_helper_collections(client)
    coll_names = client["cardano_silver"].list_collection_names()
    if "helper_entity_graph" in coll_names:
        helper_entity_graph.drop()
    if "helper_deg_dist" in coll_names:
        helper_deg_dist.drop()
    if "helper_wealth" in coll_names:
        helper_wealth.drop()
    if "helper_adr_dist" in coll_names:
        helper_adr_dist.drop()

def get_client():
    return MongoClient("172.23.149.210", 27017)

