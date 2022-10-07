#!/usr/bin/env python
# coding: utf-8
import pymongo
from pymongo import MongoClient
from pyspark.sql.functions import col, lit, when, from_json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType,BooleanType,StructField,LongType
import pyspark
import pandas as pd
from pyspark.sql import SparkSession


client = MongoClient("172.23.149.210", 27017)
db = client['cardano_bronze']

# +
# import required collections
tx = db["node.public.tx"]
tx_in = db["node.public.tx_in"]
tx_out = db["node.public.tx_out"]
last_ind = db["last_indexes_1_transactions_network"]

# import required temporary collections to overwrite with new data
tx_tmp = db["tx_temporary"]
tx_in_tmp = db["tx_in_temporary"]
tx_out_tmp = db["tx_out_temporary"]
# -

#retrieve the last indices that were processed before
tx_last_processed = last_ind.find_one({'collection': 'tx'})['last_index']
tx_in_last_processed = last_ind.find_one({'collection': 'tx_in'})['last_index']
tx_out_last_processed = last_ind.find_one({'collection': 'tx_out'})['last_index']

# count how many documents are in each new input mongodb collection
count_tx = tx.estimated_document_count()
count_tx_in = tx_in.estimated_document_count()
count_tx_out = tx_out.estimated_document_count()

# for each Cardano table, select the records which haven't been processed yet (range between last_processed and total records count)
tx_df = tx.find()[tx_last_processed:count_tx]
tx_in_df = tx_in.find()[tx_in_last_processed:count_tx_in]
tx_out_df = tx_out.find()[tx_out_last_processed:count_tx_out]

# drop the previous records in the temporary collections
tx_tmp.drop()
tx_in_tmp.drop()
tx_out_tmp.drop()

# load the temporary records in the temporary collections
tx_tmp.insert_many(tx_df)

tx_in_tmp.insert_many(tx_in_df)
tx_out_tmp.insert_many(tx_out_df)

# ('spark.driver.maxResultSize','0'): if the result is too big to be computed by the driver, then by setting the value
# to 0, we are giving the driver infinite memory to compute the result.
config = pyspark.SparkConf().setAll([
    ('spark.driver.extraJavaOptions', '-Djava.io.tmpdir=/home/ubuntu/notebook/tmp_spark_env'),
    ('spark.executor.memory', '30g'), 
    ('spark.executor.cores', '3'), 
    ('spark.cores.max', '3'),
    ('spark.driver.memory','10g'),
    ('spark.driver.maxResultSize','0'),
    ('spark.executor.instances', '1'),
    ('spark.dynamicAllocation.enabled', 'true'),
    ('spark.dynamicAllocation.shuffleTracking.enabled', 'true'),
    ('spark.dynamicAllocation.executorIdleTimeout', '60s'),
    ('spark.dynamicAllocation.minExecutors', '1'),
    ('spark.dynamicAllocation.maxExecutors', '4'),
    ('spark.dynamicAllocation.initialExecutors', '1'),
    ('spark.dynamicAllocation.executorAllocationRatio', '1'),
    ('spark.sql.shuffle.partitions','2048'),
    ('spark.shuffle.compress','true'),
    ('spark.default.parallelism','300'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.shuffle.service.db.enabled', 'true'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
])

spark = SparkSession \
    .builder \
    .config(conf=config) \
    .appName("MongoDB-query-network") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# import tables from mongodb
tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

tx_in = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_in_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

tx_out = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_out_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

tx.createOrReplaceTempView("tx")
tx_in.createOrReplaceTempView("tx_in")
tx_out.createOrReplaceTempView("tx_out")

inputs = " \
    select distinct tx.hash as tx_hash, tx_out.address,tx_out.tx_id,tx_out.value as input_value \
    from tx_out \
    inner join tx_in on tx_out.tx_id = tx_in.tx_out_id \
    inner join tx on tx.id = tx_in.tx_in_id and tx_in.tx_out_index = tx_out.index"

outputs = "SELECT distinct tx.hash, tx_out.tx_id, tx_out.address_raw, tx_out.address as address_output, \
tx_out.index, tx_out.tx_id, tx_out.value as output_value \
FROM tx_out \
INNER JOIN tx ON tx_out.tx_id = tx.id"

spark.sql(inputs).createOrReplaceTempView("inputs_table")
spark.sql(outputs).createOrReplaceTempView("outputs_table")

join_outputs_inputs = "SELECT distinct inputs_table.tx_hash, \
        inputs_table.address AS input_addr, \
       outputs_table.address_output AS output_addr, \
         inputs_table.input_value / 1000000 AS input_ADA_value, \
         outputs_table.output_value / 1000000 AS output_ADA_value \
FROM   inputs_table \
       LEFT JOIN outputs_table \
              ON inputs_table.tx_hash = outputs_table.HASH \
WHERE  inputs_table.address != outputs_table.address_output"

spark.sql(join_outputs_inputs).createOrReplaceTempView("final_table")

result = spark.sql(join_outputs_inputs).coalesce(1)

result.write.format("mongodb") \
 .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .mode("append") \
    .option('spark.mongodb.database', 'cardano_silver') \
   .option('spark.mongodb.collection', 'transaction_network') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .save()

# +
# update the old checkpoints with new ones, based on current document count
# TODO: this checkpoint save step has to be done AFTER the new records have been added in the network table
tx_query = { "collection": "tx" }
new_tx_count = { "$set": { "last_index": count_tx } }

tx_in_query = { "collection": "tx_in" }
new_tx_in_count = { "$set": { "last_index": count_tx_in } }

tx_out_query = { "collection": "tx_out" }
new_tx_out_count = { "$set": { "last_index": count_tx_out } }

last_ind.update_one(tx_query, new_tx_count)
last_ind.update_one(tx_in_query, new_tx_in_count)
last_ind.update_one(tx_out_query, new_tx_out_count)
# -

spark.stop()


