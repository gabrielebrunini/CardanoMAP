#!/usr/bin/env python
# coding: utf-8

import pymongo
from pymongo import MongoClient


client = MongoClient("172.23.149.210", 27017)
db = client['cardano_bronze']

tx = db["node.public.tx"]
tx_in = db["node.public.tx_in"]
tx_out = db["node.public.tx_out"]
last_ind = db["last_indexes"]

start_index_tx_out = int(last_ind.find_one({"collection":"tx_out"})["last_index"])
start_index_tx_in = int(last_ind.find_one({"collection":"tx_in"})["last_index"])

end_index_tx_out = int(tx_out.find_one(sort=[("id", -1)])["id"])
end_index_tx_in = int(tx_in.find_one(sort=[("id", -1)])["id"])

#Add outputs to the transaction document which they belong to
counter = 0
for i in range(start_index_tx_out, end_index_tx_out, 1):
    doc = tx_out.find_one({"id":i})
    if doc is None: continue
    tx_id = doc["tx_id"]

    myquery = {"id":tx_id}
    newvalues = {"$addToSet": {"outputs": doc}}
    tx.update_one(myquery, newvalues)

    counter += 1
    if counter % 1000 == 0:
        myquery = {"progress":"tx_out"}
        newvalues = {"$set": {"counter": (start_index_tx_out+counter)}}
        last_ind.update_one(myquery, newvalues)

    last_ind.update_one({"collection":"tx_out"}, {"$set": {"last_index": i}})


counter = 0
for i in range(start_index_tx_in, start_index_tx_in, 1):
    doc = tx_in.find_one({"id":i})
    if doc is None: continue

    tx_id = doc["tx_in_id"]
    myquery = {"id":tx_id}
    utxo = tx_out.find({"tx_id":doc["tx_out_id"], "index":doc["tx_out_index"]})
    newvalues = {"$addToSet": {"inputs": utxo.next()}}
    tx.update_one(myquery, newvalues)
    counter += 1
    if counter % 1000 == 0:
        myquery = {"progress":"tx_in"}
        newvalues = {"$set": {"counter": (start_index_tx_in + counter)}}
        last_ind.update_one(myquery, newvalues)

    last_ind.update_one({"collection":"tx_in"}, {"$set": {"last_index": i}})