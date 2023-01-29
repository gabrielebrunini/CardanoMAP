#!/usr/bin/env python
# coding: utf-8
# %%
import pymongo
from pymongo import MongoClient
import pyspark
from pyspark.sql.types import  *
from pyspark.sql import SparkSession


# %%
client = MongoClient("172.23.149.210", 27017)
db = client['cardano_silver']
db2 = client['cardano_bronze']

# %% [markdown]
# ### Create MongoDB Collections for Address Update

# %%
# import the required collections with the last checkpoint
tx_out = db2["node.public.tx_out"]
addr_last_ind = db2["last_index_address"]

# import the required temporary collection to overwrite it with new data
addr_tmp = db2["address_temporary"]

# %%
# retrieve the last indices that were processed before
addr_last_processed = addr_last_ind.find_one({'collection': 'address'})['last_index']

# %%
# count how many documents are in each new input mongodb collection
count_addr = tx_out.estimated_document_count()

# %%
# select the records which haven't been processed yet (range between addr_last_processed and total records count)
addr_df = tx_out.find()[addr_last_processed:count_addr]

# %%
# drop the previous records in the temporary collections
addr_tmp.drop()

# %%
# load the temporary records in the temporary collections
addr_tmp.insert_many(addr_df)

# %% [markdown]
# ### Start Spark Session

# %%
config = pyspark.SparkConf().setAll([
    ('spark.executor.memory', '30g'), 
    ('spark.executor.cores', '5'), # number of cores to use on each executor
    ('spark.cores.max', '15'), # the maximum amount of CPU cores to request for the application from across the cluster
    ('spark.driver.memory','20g'),
    ('spark.driver.maxResultSize', '4g'),
    ('spark.executor.instances', '3'),
    ('spark.worker.cleanup.enabled', 'true'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2'),
    ('spark.mongodb.output.writeConcern.wTimeoutMS','1000000'),
    ('spark.mongodb.output.writeConcern.socketTimeoutMS','1000000'),
    ('spark.mongodb.output.writeConcern.connectTimeoutMS','1000000'),
    ("neo4j.url", "bolt://172.23.149.210:7687"),
    ("neo4j.authentication.type", "basic"),
    ("neo4j.authentication.basic.username", "neo4j"),
    ("neo4j.authentication.basic.password", "cardano")
])

# %%
spark = SparkSession \
    .builder \
    .config(conf=config) \
    .appName("Neo4j-Stream-Address-Update") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %% [markdown]
# #### Define schema for the different collections

# %%
schema = StructType([ \
    StructField("address", StringType(), True) \
])

# %%
schema2 = StructType([ \
    StructField("address", StringType(), True), \
    StructField("id", IntegerType(), True) \
])

# %%
schema3 = StructType([ \
    StructField("input_addr", StringType(), True), \
    StructField("output_addr", StringType(), True), \
    StructField("tx_hash", StringType(), True), \
    StructField("input_ADA_value", IntegerType(), True), \
    StructField("output_ADA_value", IntegerType(), True), \
])

# %%
schema4 = StructType([ \
    StructField("hash", StringType(), True), \
    StructField("id", IntegerType(), True) \
])

# %% [markdown]
# ### Address Update

# %%
temp_addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'address_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema) \
  	.load()

# %%
temp_addresses.createOrReplaceTempView("temp_addresses")

# %%
temp_addresses2 = spark.sql("SELECT DISTINCT address FROM temp_addresses")

# %%
temp_addresses2.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'address_temporary_2') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
temp_addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'address_temporary_2') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema) \
  	.load()

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")
temp_addresses.createOrReplaceTempView("temp_addresses")

# %%
new_addresses = spark.sql("SELECT address FROM temp_addresses WHERE NOT EXISTS (SELECT address FROM addresses)")


# %%
new_addresses.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'new_addresses_temporary') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
# find first address id for new addresses collection
addr = db["addresses"]
count = addr.estimated_document_count()

# %%
# create ids for the new addresses
collection = db2.new_addresses_temporary.find()
for doc in collection:
    update = {'$set': {"id": count}}
    db2.new_addresses_temporary.update_one(doc, update)
    count += 1

# %%
# read the new addresses with their ids
final_new_addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'new_addresses_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema2) \
  	.load()

# %%
# append new unique addresses to the existing collection of addresses
final_new_addresses.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_silver') \
   .option('spark.mongodb.collection', 'addresses') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
# stream new addresses to neo4j
final_new_addresses.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("labels", "Address") \
  .option("node.keys", "id") \
  .save()

# %%
# drop temporary collection
db2.address_temporary_2.drop()
db2.new_addresses_temporary.drop()

# %%
# update checkpoints
addr_query = {"collection": "address"}
new_count = {"$set":{"last_index": count_addr}}
addr_last_ind.update_one(addr_query, new_count)

# %%
spark.stop()

# %% [markdown]
# ###  Create MongoDB Collections for Networks Update

# %%
# import required collections
tx = db["transaction_network"]
tx_last_ind = db2["last_index_neo4j_stream"]

# import required temporary collections to overwrite with new data
tx_tmp = db2["neo4j_stream_temporary"]

# %%
#retrieve the last indices that were processed before
tx_last_processed = tx_last_ind.find_one({'collection': 'neo4j_stream'})['last_index']

# %%
# count how many documents are in each new input mongodb collection
count_tx = tx.estimated_document_count()

# %%
# for each Cardano table, select the records which haven't been processed yet (range between last_processed and total records count)
tx_df = tx.find()[tx_last_processed:count_tx]

# %%
# drop the previous records in the temporary collections
tx_tmp.drop()

# %%
# load the temporary records in the temporary collections
tx_tmp.insert_many(tx_df)

# %% [markdown]
# ### Start Spark Session
#

# %%
config = pyspark.SparkConf().setAll([
    ('spark.executor.memory', '30g'), 
    ('spark.executor.cores', '5'), # number of cores to use on each executor
    ('spark.cores.max', '15'), # the maximum amount of CPU cores to request for the application from across the cluster
    ('spark.driver.memory','20g'),
    ('spark.driver.maxResultSize', '4g'),
    ('spark.executor.instances', '3'),
    ('spark.worker.cleanup.enabled', 'true'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2'),
    ('spark.mongodb.output.writeConcern.wTimeoutMS','1000000'),
    ('spark.mongodb.output.writeConcern.socketTimeoutMS','1000000'),
    ('spark.mongodb.output.writeConcern.connectTimeoutMS','1000000'),
    ("neo4j.url", "bolt://172.23.149.210:7687"),
    ("neo4j.authentication.type", "basic"),
    ("neo4j.authentication.basic.username", "neo4j"),
    ("neo4j.authentication.basic.password", "cardano")
])

# %%
spark = SparkSession \
    .builder \
    .config(conf=config) \
    .appName("Neo4j-Stream-Networks-Update") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %% [markdown]
# #### Define schema for the different collections

# %%
schema = StructType([ \
    StructField("address", StringType(), True) \
])

# %%
schema2 = StructType([ \
    StructField("address", StringType(), True), \
    StructField("id", IntegerType(), True) \
])

# %%
schema3 = StructType([ \
    StructField("input_addr", StringType(), True), \
    StructField("output_addr", StringType(), True), \
    StructField("tx_hash", StringType(), True), \
    StructField("input_ADA_value", IntegerType(), True), \
    StructField("output_ADA_value", IntegerType(), True), \
])

# %%
schema4 = StructType([ \
    StructField("hash", StringType(), True), \
    StructField("id", IntegerType(), True) \
])

# %% [markdown]
# ### Transaction Network & Clusters Update

# %%
TxNetwork = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema3) \
  	.load()

# %%
TxNetwork.createOrReplaceTempView("TxNetwork")

# %%
tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.tx') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
tx.createOrReplaceTempView("tx")

# %%
transactions_id = spark.sql("SELECT tx_hash, id as tx_id, input_addr, output_addr, input_ADA_value, output_ADA_value FROM TxNetwork LEFT JOIN tx on TxNetwork.tx_hash = tx.hash")

# %%
transactions_id.createOrReplaceTempView("transactions_id")

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema2) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")

# %%
input_addresses = spark.sql("SELECT transactions_id.tx_hash, transactions_id.tx_id, transactions_id.input_addr, transactions_id.output_addr, transactions_id.input_ADA_value, transactions_id.output_ADA_value, addresses.id as input_id FROM transactions_id LEFT JOIN addresses on transactions_id.input_addr = addresses.address")


# %%
input_addresses.createOrReplaceTempView("input_addresses")

# %%
TxNet = spark.sql("SELECT input_addresses.tx_hash, input_addresses.tx_id, input_addresses.input_addr, input_addresses.output_addr, input_addresses.input_ADA_value, input_addresses.output_ADA_value, input_addresses.input_id, addresses.id as output_id FROM input_addresses LEFT JOIN addresses on input_addresses.output_addr = addresses.address")


# %% [markdown]
# ### Neo4j Tx Network & Clusters Streaming

# %%
# stream transaction network
TxNet.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input_id}) MATCH (a2:Address {id: event.output_id}) CREATE (a1)-[:TRANSACTED_WITH {tx_hash: event.tx_hash, tx_id: event.tx_id, input_ADA_value: event.input_ADA_value, output_ADA_value: event.output_ADA_value}]->(a2)") \
  .save()

# %%
TxNet.createOrReplaceTempView("TxNet")

# %%
clusters = spark.sql("SELECT t1.tx_id, t1.input_id as input1, t2.input_id as input2 FROM TxNet t1 LEFT JOIN TxNet t2 on t1.tx_id = t2.tx_id WHERE t1.input_id != t2.input_id")


# %%
clusters.createOrReplaceTempView("clusters")

# %%
clusters_final = spark.sql("SELECT distinct tx_id, input1, input2 FROM clusters")

# %%
# stream clusering network
clusters_final.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input1}) MATCH (a2:Address {id: event.input2}) CREATE (a1)-[:ASSOCIATED_WITH]->(a2)") \
  .save()

# %%
# update checkpoints
tx_query = {"collection": "neo4j_stream"}
new_count = {"$set":{"last_index": count_tx}}
tx_last_ind.update_one(tx_query, new_count)

# %%
spark.stop()

# %%
