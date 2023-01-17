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

# %% tags=[]
import pymongo
from pymongo import MongoClient
import pyspark
from pyspark.sql.types import  *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# %%
client = MongoClient("172.23.149.210", 27017)
db = client['cardano_silver']
db2 = client['cardano_bronze']

# %%
config = pyspark.SparkConf().setAll([
    ('spark.executor.memory', '90g'), 
    ('spark.executor.cores', '5'), # number of cores to use on each executor
    ('spark.cores.max', '5'), # the maximum amount of CPU cores to request for the application from across the cluster
    ('spark.driver.memory','20g'),
    ('spark.executor.instances', '1'),
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
    .appName("Neo4j-Stream") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %%
schema = StructType([ \
    StructField("address", StringType(), True) \
])

# %%
schema2 = StructType([ \
    StructField("input_addr", StringType(), True), \
    StructField("output_addr", StringType(), True), \
    StructField("tx_hash", StringType(), True), \
    StructField("input_ADA_value", IntegerType(), True), \
    StructField("output_ADA_value", IntegerType(), True), \
])

# %%
schema3 = StructType([ \
    StructField("address", StringType(), True), \
    StructField("id", IntegerType(), True) \
])

# %%
schema4 = StructType([ \
    StructField("input_addr", StringType(), True), \
    StructField("output_addr", StringType(), True), \
    StructField("tx_hash", StringType(), True), \
    StructField("tx_id", IntegerType(), True), \
    StructField("input_ADA_value", IntegerType(), True), \
    StructField("output_ADA_value", IntegerType(), True), \
])

# %%
schema5 = StructType([ \
    StructField("hash", StringType(), True), \
    StructField("id", IntegerType(), True) \
])

# %%
schema6 = StructType([ \
    StructField("input_addr", StringType(), True), \
    StructField("output_addr", StringType(), True), \
    StructField("tx_hash", StringType(), True), \
    StructField("tx_id", IntegerType(), True), \
    StructField("input_ADA_value", IntegerType(), True), \
    StructField("output_ADA_value", IntegerType(), True), \
    StructField("input_id", IntegerType(), True), \
    StructField("output_id", IntegerType(), True), \
])

# %%
tx_out = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.tx_out') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema) \
  	.load()

# %%
tx_out.createOrReplaceTempView("tx_out")

# %%
addresses_raw = spark.sql("SELECT DISTINCT address FROM tx_out")

# %%
addresses_raw.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'addresses') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
addresses_raw.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("labels", "Address") \
  .option("node.keys", "address") \
  .save()

# %%
# Create id for each address in neo4j using internal ids using:
# CALL apoc.periodic.iterate("MATCH (n:Address) RETURN n","SET n.id = id(n)",{batchSize:10000, parallel:false})

# %%
addresses_id = spark.read.format("org.neo4j.spark.DataSource") \
  .option("labels", "Address") \
  .load()

# %%
addresses_id.createOrReplaceTempView("addresses_id")

# %%
addresses_id = spark.sql("SELECT id, address FROM addresses_id")

# %%
addresses_id.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_silver') \
   .option('spark.mongodb.collection', 'addresses') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %% [markdown]
# ### Transaction Network Processing

# %%
# read only the tx id and hash
tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.tx') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema5) \
  	.load()

# %%
# store the tx id and hash
tx.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'tx_ids') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
# read the stored collection
tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_ids') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema5) \
  	.load()

# %%
tx.createOrReplaceTempView("tx")

# %%
TxNetwork = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'transaction_network') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema2) \
  	.load()

# %%
TxNetwork.createOrReplaceTempView("TxNetwork")

# %%
# join the transaction network to tx in order to add tx_id to the transaction network collection
transactions_id = spark.sql("SELECT tx_hash, id as tx_id, input_addr, output_addr, input_ADA_value, output_ADA_value FROM TxNetwork LEFT JOIN tx on TxNetwork.tx_hash = tx.hash")


# %%
transactions_id.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage1') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
transactions_id = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
# divide the transactions to avoid memory or storage related issues
transactions_id_1 = transactions_id.filter(col("tx_id").between(0,12000000))

# %%
transactions_id_1.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage2_1') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema3) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")

# %%
transactions_id_1 = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage2_1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_1.createOrReplaceTempView("transactions_id_1")

# %%
# join transaction network to addresses to get the input address ids
input_addresses = spark.sql("SELECT transactions_id_1.tx_hash, transactions_id_1.tx_id, transactions_id_1.input_addr, transactions_id_1.output_addr, transactions_id_1.input_ADA_value, transactions_id_1.output_ADA_value, addresses.id as input_id FROM transactions_id_1 LEFT JOIN addresses on transactions_id_1.input_addr = addresses.address")


# %%
input_addresses.createOrReplaceTempView("input_addresses")

# %%
# join the previous dataframe to addresses to get the output ids
TxNet = spark.sql("SELECT input_addresses.tx_hash, input_addresses.tx_id, input_addresses.input_addr, input_addresses.output_addr, input_addresses.input_ADA_value, input_addresses.output_ADA_value, input_addresses.input_id, addresses.id as output_id FROM input_addresses LEFT JOIN addresses on input_addresses.output_addr = addresses.address")


# %%
# store the result, which includes the original transaction network with columns for tx_id, input addr. id, 
# and output addr. id to make neo4j queries more effecient
TxNet.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage3_1') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
# repeat the previous process with a new part of the original data
transactions_id = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_2 = transactions_id.filter(col("tx_id").between(12000001,28000000))

# %%
transactions_id_2.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage2_2') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema3) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")

# %%
transactions_id_2 = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage2_2') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_2.createOrReplaceTempView("transactions_id_2")

# %%
input_addresses = spark.sql("SELECT transactions_id_2.tx_hash, transactions_id_2.tx_id, transactions_id_2.input_addr, transactions_id_2.output_addr, transactions_id_2.input_ADA_value, transactions_id_2.output_ADA_value, addresses.id as input_id FROM transactions_id_2 LEFT JOIN addresses on transactions_id_2.input_addr = addresses.address")


# %%
input_addresses.createOrReplaceTempView("input_addresses")

# %%
TxNet = spark.sql("SELECT input_addresses.tx_hash, input_addresses.tx_id, input_addresses.input_addr, input_addresses.output_addr, input_addresses.input_ADA_value, input_addresses.output_ADA_value, input_addresses.input_id, addresses.id as output_id FROM input_addresses LEFT JOIN addresses on input_addresses.output_addr = addresses.address")


# %%
TxNet.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage3_2') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
transactions_id = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_3 = transactions_id.filter(col("tx_id").between(28000001,34000000))

# %%
transactions_id_3.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage2_3') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema3) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")

# %%
transactions_id_3 = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage2_3') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_3.createOrReplaceTempView("transactions_id_3")

# %%
input_addresses = spark.sql("SELECT transactions_id_3.tx_hash, transactions_id_3.tx_id, transactions_id_3.input_addr, transactions_id_3.output_addr, transactions_id_3.input_ADA_value, transactions_id_3.output_ADA_value, addresses.id as input_id FROM transactions_id_3 LEFT JOIN addresses on transactions_id_3.input_addr = addresses.address")


# %%
input_addresses.createOrReplaceTempView("input_addresses")

# %%
TxNet = spark.sql("SELECT input_addresses.tx_hash, input_addresses.tx_id, input_addresses.input_addr, input_addresses.output_addr, input_addresses.input_ADA_value, input_addresses.output_ADA_value, input_addresses.input_id, addresses.id as output_id FROM input_addresses LEFT JOIN addresses on input_addresses.output_addr = addresses.address")


# %%
TxNet.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage3_3') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
transactions_id = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_4 = transactions_id.filter(col("tx_id").between(34000001,40000000))

# %%
transactions_id_4.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage2_4') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema3) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")

# %%
transactions_id_4 = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage2_4') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_4.createOrReplaceTempView("transactions_id_4")

# %%
input_addresses = spark.sql("SELECT transactions_id_4.tx_hash, transactions_id_4.tx_id, transactions_id_4.input_addr, transactions_id_4.output_addr, transactions_id_4.input_ADA_value, transactions_id_4.output_ADA_value, addresses.id as input_id FROM transactions_id_4 LEFT JOIN addresses on transactions_id_4.input_addr = addresses.address")


# %%
input_addresses.createOrReplaceTempView("input_addresses")

# %%
TxNet = spark.sql("SELECT input_addresses.tx_hash, input_addresses.tx_id, input_addresses.input_addr, input_addresses.output_addr, input_addresses.input_ADA_value, input_addresses.output_ADA_value, input_addresses.input_id, addresses.id as output_id FROM input_addresses LEFT JOIN addresses on input_addresses.output_addr = addresses.address")


# %%
TxNet.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage3_4') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
transactions_id = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_5 = transactions_id.filter(col("tx_id").between(40000001,58400000))

# %%
transactions_id_5.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage2_5') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %%
addresses = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addresses') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema3) \
  	.load()

# %%
addresses.createOrReplaceTempView("addresses")

# %%
transactions_id_5 = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage2_5') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema4) \
  	.load()

# %%
transactions_id_5.createOrReplaceTempView("transactions_id_5")

# %%
input_addresses = spark.sql("SELECT transactions_id_5.tx_hash, transactions_id_5.tx_id, transactions_id_5.input_addr, transactions_id_5.output_addr, transactions_id_5.input_ADA_value, transactions_id_5.output_ADA_value, addresses.id as input_id FROM transactions_id_5 LEFT JOIN addresses on transactions_id_5.input_addr = addresses.address")


# %%
input_addresses.createOrReplaceTempView("input_addresses")

# %%
TxNet = spark.sql("SELECT input_addresses.tx_hash, input_addresses.tx_id, input_addresses.input_addr, input_addresses.output_addr, input_addresses.input_ADA_value, input_addresses.output_ADA_value, input_addresses.input_id, addresses.id as output_id FROM input_addresses LEFT JOIN addresses on input_addresses.output_addr = addresses.address")


# %%
TxNet.write.format("mongodb") \
   .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'neo4j_stream_stage3_5') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# %% [markdown]
# ## Neo4j Streaming

# %% [markdown]
# ### Part 1

# %%
TxNet = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage3_1') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema6) \
  	.load()

# %%
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
clusters_final.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input1}) MATCH (a2:Address {id: event.input2}) CREATE (a1)-[:ASSOCIATED_WITH]->(a2)") \
  .save()

# %% [markdown]
# ### Part 2

# %%
TxNet = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage3_2') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema6) \
  	.load()

# %%
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
clusters_final.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input1}) MATCH (a2:Address {id: event.input2}) CREATE (a1)-[:ASSOCIATED_WITH]->(a2)") \
  .save()

# %% [markdown]
# ### Part 3

# %%
TxNet = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage3_3') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema6) \
  	.load()

# %%
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
clusters_final.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input1}) MATCH (a2:Address {id: event.input2}) CREATE (a1)-[:ASSOCIATED_WITH]->(a2)") \
  .save()

# %% [markdown]
# ### Part 4

# %%
TxNet = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage3_4') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema6) \
  	.load()

# %%
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
clusters_final.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input1}) MATCH (a2:Address {id: event.input2}) CREATE (a1)-[:ASSOCIATED_WITH]->(a2)") \
  .save()

# %% [markdown]
# ### Part 5

# %%
TxNet = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'neo4j_stream_stage3_5') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
    .schema(schema6) \
  	.load()

# %%
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
clusters_final.write.format("org.neo4j.spark.DataSource") \
  .mode("append") \
  .option("url", "bolt://172.23.149.210:7687") \
  .option("query","MATCH (a1:Address {id: event.input1}) MATCH (a2:Address {id: event.input2}) CREATE (a1)-[:ASSOCIATED_WITH]->(a2)") \
  .save()

# %%
last_ind = db2["last_index_neo4j_stream"]
addr_query = {"collection": "neo4j_stream"}
new_count = {"$set":{"last_index": count_records}}
last_ind.update_one(addr_query, new_count)

# %%
spark.stop()
