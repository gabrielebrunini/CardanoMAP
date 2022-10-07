# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pymongo
from pymongo import MongoClient
from pyspark.sql.functions import col, lit, when, from_json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType,BooleanType,StructField,LongType
import pyspark
import pandas as pd
from pyspark.sql import SparkSession

# %%
client = MongoClient("172.23.149.210", 27017)
db = client['cardano_bronze']

# %%
tx = db["node.public.tx"]
redeemer = db["node.public.redeemer"]
tx_in = db["node.public.tx_in"]
tx_out = db["node.public.tx_out"]
block = db["node.public.block"]

last_ind = db["last_indexes_4_smart_contracts"]

# import required temporary collections to overwrite with new data
tx_tmp = db["tx_temporary"]
redeemer_tmp = db["redeemer_temporary"]
tx_in_tmp = db["tx_in_temporary"]
tx_out_tmp = db["tx_out_temporary"]
block_tmp = db["block_temporary"]

# %%
# insert initial values in checkpoint table
#initial_val_tx = { "collection": "tx", "last_index": 0 }
#initial_val_redeemer = { "collection": "redeemer", "last_index": 0 }
#initial_val_tx_in = { "collection": "tx_in", "last_index": 0 }
#initial_val_tx_out = { "collection": "tx_out", "last_index": 0 }
#initial_val_block = { "collection": "block", "last_index": 0 }
#
#last_ind.insert_one(initial_val_tx)
#last_ind.insert_one(initial_val_redeemer)
#last_ind.insert_one(initial_val_tx_in)
#last_ind.insert_one(initial_val_tx_out)
#last_ind.insert_one(initial_val_block)

# %%
#retrieve the last indices that were processed before, by index (first: tx, second: tx_in, third: tx_out)
tx_last_processed = last_ind.find_one({'collection': 'tx'})['last_index']
redeemer_last_processed = last_ind.find_one({'collection': 'redeemer'})['last_index']
tx_in_last_processed = last_ind.find_one({'collection': 'tx_in'})['last_index']
tx_out_last_processed = last_ind.find_one({'collection': 'tx_out'})['last_index']
block_last_processed = last_ind.find_one({'collection': 'block'})['last_index']

# %%
# count how many documents are in each new input mongodb collection
count_tx = tx.estimated_document_count()
count_redeemer = redeemer.estimated_document_count()
count_tx_in = tx_in.estimated_document_count()
count_tx_out = tx_out.estimated_document_count()
count_block = block.estimated_document_count()

# %%
#tx_df = tx.find()[0:1000]
#redeemer_df = redeemer.find()[0:1000]
#tx_in_df = tx_in.find()[0:1000]
#tx_out_df = tx_out.find()[0:1000]
#block_df = block.find()[0:1000]

# %%
tx_df = tx.find()[tx_last_processed:count_tx]
redeemer_df = redeemer.find()[redeemer_last_processed:count_redeemer]
tx_in_df = tx_in.find()[tx_in_last_processed:count_tx_in]
tx_out_df = tx_out.find()[tx_out_last_processed:count_tx_out]
block_df = block.find()[block_last_processed:count_block]

# %%
# drop the previous records in the temporary collections
tx_tmp.drop()
redeemer_tmp.drop()
tx_in_tmp.drop()
tx_out_tmp.drop()
block_tmp.drop()

# %%
# load the temporary records in the temporary collections
tx_tmp.insert_many(tx_df)
redeemer_tmp.insert_many(redeemer_df)
tx_in_tmp.insert_many(tx_in_df)
tx_out_tmp.insert_many(tx_out_df)
block_tmp.insert_many(block_df)

# %%
# save Spark temporary files into folder /home/ubuntu/notebook/tmp_spark_files, with more space
config = pyspark.SparkConf().setAll([
    #('spark.driver.extraJavaOptions', '-Djava.io.tmpdir=/home/ubuntu/notebook/tmp_spark_env'),
    ('spark.executor.memory', '30g'),
    ('spark.executor.cores', '3'),
    ('spark.cores.max', '3'),
    ('spark.driver.memory','10g'),
    ('spark.executor.instances', '2'),
    ('spark.worker.cleanup.enabled', 'true'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2'),
    ('spark.mongodb.output.writeConcern.wTimeoutMS','120000'),
    ('spark.mongodb.output.writeConcern.socketTimeoutMS','120000'),
    ('spark.mongodb.output.writeConcern.connectTimeoutMS','120000')
])

# %% tags=[]
spark = SparkSession \
    .builder \
	.config(conf=config) \
    .appName("MongoDB-query-smart-contracts") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %% [markdown] tags=[]
# # Tables needed

# %% [markdown]
# - tx
# - redeemer
# - tx_in
# - tx_out

# %% [raw]
# #### READ FROM CSV AS TEST - need to load redeemer table from mongoDB
# tx = spark.read.json("/home/ubuntu/mock_data/shared_data/tx_10k.json")
# tx_in = spark.read.json("/home/ubuntu/mock_data/shared_data/tx_in_10k.json")
# tx_out = spark.read.json("/home/ubuntu/mock_data/shared_data/tx_out_10k.json")

# %%
tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
redeemer = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'redeemer_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
tx_in = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_in_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
tx_out = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_out_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
block = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'block_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
block = block.withColumn("time", col("time")/1000000).select("id","hash", "block_no","tx_count","id","epoch_no","slot_no",
    F.from_unixtime("time",'yyyy-MM-dd').alias('date'))

# %%
tx.createOrReplaceTempView("tx")
redeemer.createOrReplaceTempView("redeemer")
tx_in.createOrReplaceTempView("tx_in")
tx_out.createOrReplaceTempView("tx_out")
block.createOrReplaceTempView("block")

# %% [markdown] tags=[]
# # Query

# %% [markdown]
# Retrieve the total transaction output value, the total budget in Memory to run a script, the total budget in CPU steps to run a script, and the total budget in fees to run a script, given a spend-purpose script. 

# %%
query_smart_contracts = "select redeemer.script_hash, sum(tx_out.value)/1000000 as tx_out_value, \
    sum(redeemer.unit_mem) as tot_unit_mem, \
    sum(redeemer.unit_steps) as tot_unit_steps, sum(redeemer.fee)/1000000 as total_fees, \
    first(block.date) as deployment_timestamp, \
    first(block.block_no) as block_no, \
    first(block.hash) as block_hash \
  from tx join redeemer on redeemer.tx_id = tx.id \
  join tx_in on tx_in.redeemer_id = redeemer.id \
  join tx_out on tx_in.tx_out_id = tx_out.tx_id and tx_in.tx_out_index = tx_out.index \
  join block on block.block_no = tx.block_id \
  group by redeemer.script_hash"

# %%
spark.sql(query_smart_contracts).createOrReplaceTempView("query_smart_contracts")

# %%
SC_result = spark.sql(query_smart_contracts)

# %%
## Write in the GOLD db, collection smart_contract_calls
SC_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("append") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'smart_contract_info') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
### EXECUTE THE SECONDARY QUERIES ON THE FULL TABLES

# %%
query_smart_contracts = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'smart_contract_info') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
query_smart_contracts.createOrReplaceTempView("query_smart_contracts")

# %% [markdown]
# ### Average fee, tx_out, unit_mem and unit_steps  per contract

# %%
avg_fee = "select avg(total_fees)/1000000 ADA_avg_fees, avg(tx_out_value)/1000000 ADA_avg_value, avg(tot_unit_mem) avg_unit_mem, \
                avg(tot_unit_steps) avg_unit_steps \
                from query_smart_contracts"

# %%
avg_fee_result = spark.sql(avg_fee)

# %%
spark.sql(avg_fee).createOrReplaceTempView("avg_fee")

# %%
## Write in the GOLD db, collection avg_fee_per_contract
avg_fee_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'avg_fee_per_contract') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %% [markdown]
# ### Number of contracts per block

# %%
count_sc_block = "select block_no, count(*) AS cnt_sc  \
                from query_smart_contracts \
                group by block_no"

# %%
count_sc_block_result = spark.sql(count_sc_block)

# %%
spark.sql(count_sc_block).createOrReplaceTempView("count_sc_block")

# %%
## Write in the GOLD db, collection count_SC_per_block
count_sc_block_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'count_SC_per_block') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %% [markdown]
# ### Number of contracts by date

# %%
sc_by_date = " select count(*) as nr_sc_created, \
                deployment_timestamp \
                from query_smart_contracts \
                GROUP BY deployment_timestamp"

# %%
sc_by_date_result = spark.sql(sc_by_date)

# %%
## Write in the GOLD db, collection count_SC_per_block
sc_by_date_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'sc_call_by_date') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.stop()

# %%
# update the old checkpoints with new ones, based on current document count
# TODO: this checkpoint save step has to be done AFTER the new records have been added in the network table
tx_query = { "collection": "tx" }
new_tx_count = { "$set": { "last_index": count_tx } }

tx_in_query = { "collection": "tx_in" }
new_tx_in_count = { "$set": { "last_index": count_tx_in } }

tx_out_query = { "collection": "tx_out" }
new_tx_out_count = { "$set": { "last_index": count_tx_out } }

redeemer_query = { "collection": "redeemer" }
new_redeemer_count = { "$set": { "last_index": count_redeemer } }

block_query = { "collection": "block" }
new_block_count = { "$set": { "last_index": count_block } }

last_ind.update_one(tx_query, new_tx_count)
last_ind.update_one(tx_in_query, new_tx_in_count)
last_ind.update_one(tx_out_query, new_tx_out_count)
last_ind.update_one(redeemer_query, new_redeemer_count)
last_ind.update_one(block_query, new_block_count)
