# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# - Get all assets with quantities and count of mints

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
multi_assets = db["node.public.multi_asset"]
tx = db["node.public.tx"]
ma_tx_mint = db["node.public.ma_tx_mint"]
block = db["node.public.block"]
tx_metadata = db["node.public.tx_metadata"]

last_ind = db["last_indexes_5_multi_assets"]

# import required temporary collections to overwrite with new data
multi_assets_tmp = db["multi_assets_temporary"]
tx_tmp = db["tx_temporary"]
ma_tx_mint_tmp = db["ma_tx_mint_temporary"]
block_tmp = db["block_temporary"]
tx_metadata_tmp = db["tx_metadata_temporary"]

# +
# insert initial values in checkpoint table
#initial_val_multi_assets = { "collection": "multi_assets", "last_index": 0 }
#initial_val_tx = { "collection": "tx", "last_index": 0 }
#initial_val_ma_tx_mint = { "collection": "ma_tx_mint", "last_index": 0 }
#initial_val_block = { "collection": "block", "last_index": 0 }
#initial_val_tx_metadata = { "collection": "tx_metadata", "last_index": 0 }
#
#last_ind.insert_one(initial_val_multi_assets)
#last_ind.insert_one(initial_val_tx)
#last_ind.insert_one(initial_val_ma_tx_mint)
#last_ind.insert_one(initial_val_block)
#last_ind.insert_one(initial_val_tx_metadata)
# -

#retrieve the last indices that were processed before, by index
multi_assets_last_processed = last_ind.find_one({'collection': 'multi_assets'})['last_index']
tx_last_processed = last_ind.find_one({'collection': 'tx'})['last_index']
ma_tx_mint_last_processed = last_ind.find_one({'collection': 'ma_tx_mint'})['last_index']
block_last_processed = last_ind.find_one({'collection': 'block'})['last_index']
tx_metadata_last_processed = last_ind.find_one({'collection': 'tx_metadata'})['last_index']

# count how many documents are in each new input mongodb collection
count_multi_assets = multi_assets.estimated_document_count()
count_tx = tx.estimated_document_count()
count_ma_tx_mint = ma_tx_mint.estimated_document_count()
count_block = block.estimated_document_count()
count_tx_metadata = tx_metadata.estimated_document_count()

# for each Cardano table, select the records which haven't been processed yet (range between last_processed and total records count)
multi_assets_df = multi_assets.find()[multi_assets_last_processed:count_multi_assets]
tx_df = tx.find()[tx_last_processed:count_tx]
ma_tx_mint_df = ma_tx_mint.find()[ma_tx_mint_last_processed:count_ma_tx_mint]
block_df = block.find()[block_last_processed:count_block]
tx_metadata_df = tx_metadata.find()[tx_metadata_last_processed:count_tx_metadata]

# drop the previous records in the temporary collections
multi_assets_tmp.drop()
tx_tmp.drop()
ma_tx_mint_tmp.drop()
block_tmp.drop()
tx_metadata_tmp.drop()

#retrieve the last indices that were processed before
multi_assets_last_processed = last_ind.find_one({'collection': 'multi_assets'})['last_index']
tx_last_processed = last_ind.find_one({'collection': 'tx'})['last_index']
ma_tx_mint_last_processed = last_ind.find_one({'collection': 'ma_tx_mint'})['last_index']
block_last_processed = last_ind.find_one({'collection': 'block'})['last_index']
tx_metadata_last_processed = last_ind.find_one({'collection': 'tx_metadata'})['last_index']

# count how many documents are in each new input mongodb collection
count_multi_assets = multi_assets.estimated_document_count()
count_tx = tx.estimated_document_count()
count_ma_tx_mint = ma_tx_mint.estimated_document_count()
count_block = block.estimated_document_count()
count_tx_metadata = tx_metadata.estimated_document_count()

# for each Cardano table, select the records which haven't been processed yet (range between last_processed and total records count)
multi_assets_df = multi_assets.find()[multi_assets_last_processed:count_multi_assets]
tx_df = tx.find()[tx_last_processed:count_tx]
ma_tx_mint_df = ma_tx_mint.find()[ma_tx_mint_last_processed:count_ma_tx_mint]
block_df = block.find()[block_last_processed:count_block]

# drop the previous records in the temporary collections
multi_assets_tmp.drop()
tx_tmp.drop()
ma_tx_mint_tmp.drop()
block_tmp.drop()
tx_metadata_tmp.drop()

# load the temporary records in the temporary collections
multi_assets_tmp.insert_many(multi_assets_df)
tx_tmp.insert_many(tx_df)
ma_tx_mint_tmp.insert_many(ma_tx_mint_df)
block_tmp.insert_many(block_df)
tx_metadata_tmp.insert_many(tx_metadata_df)

# save Spark temporary files into folder /home/ubuntu/notebook/tmp_spark_files, with more space
config = pyspark.SparkConf().setAll([
    #('spark.driver.extraJavaOptions', '-Djava.io.tmpdir=/home/ubuntu/notebook/tmp_spark_files'),
    ('spark.executor.memory', '30g'),
    ('spark.executor.cores', '6'),
    ('spark.cores.max', '6'),
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

# + tags=[]
spark = SparkSession \
    .builder \
	.config(conf=config) \
    .appName("MongoDB-query-token-types") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# + [markdown] tags=[]
# # Tables needed
# -

# - multi_assets
# - ma_tx_mint
# - block
# - tx

multi_asset = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'multi_assets_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

ma_tx_mint = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'ma_tx_mint_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

#     .schema(schema_block) \
block = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'block_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

tx_metadata = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'tx_metadata_temporary') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# Convert unix date (in ms) to time in the block table

block = block.withColumn("time", col("time")/1000000).select("id", "tx_count","id","epoch_no","slot_no",
    F.from_unixtime("time",'yyyy-MM-dd').alias('date'))

multi_asset.createOrReplaceTempView("multi_asset")
ma_tx_mint.createOrReplaceTempView("ma_tx_mint")
block.createOrReplaceTempView("block")
tx.createOrReplaceTempView("tx")
tx_metadata.createOrReplaceTempView("tx_metadata")

# To be saved in an intermediate table (Silver Spark)

token_types_intermediate = "SELECT ma_tx_mint.ident, \
            sum(ma_tx_mint.quantity) AS quantity, \
            count(*) AS cnt, \
            min(ma_tx_mint.tx_id) AS mtx \
           FROM ma_tx_mint \
          GROUP BY ma_tx_mint.ident"

### INTERMEDIATE RESULTS STORED LOCALLY AS A VARIABLE
spark.sql(token_types_intermediate).createOrReplaceTempView("token_types_intermediate")

query_token_types_final = "SELECT multi_asset.fingerprint, \
    tx_metadata.key, \
    multi_asset.policy, \
    multi_asset.name, \
    token_types_intermediate.ident, \
    token_types_intermediate.cnt mints, \
    token_types_intermediate.quantity as total_supply, \
    block.date as date_created \
     FROM token_types_intermediate \
     LEFT JOIN multi_asset ON multi_asset.id = token_types_intermediate.ident \
     LEFT JOIN tx ON tx.id = token_types_intermediate.mtx \
     LEFT JOIN tx_metadata ON tx_metadata.tx_id = token_types_intermediate.mtx \
     LEFT JOIN block ON block.id = tx.block_id;"

spark.sql(query_token_types_final).createOrReplaceTempView("query_token_types_final")

result = spark.sql(query_token_types_final)

## Write in the SILVER db, collection query_token_types
result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("append") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'query_token_types') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# # Queries to store in the Gold table

query_token_types_final = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'query_token_types') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

query_token_types_final.createOrReplaceTempView("query_token_types_final")

# ### 1. Number of tokens created by day of year:
# - tokens created between 

tokens_by_day = "select query_token_types_final.date_created, count(*) tokens_created_count \
                from query_token_types_final \
                group by query_token_types_final.date_created"

result_1 = spark.sql(tokens_by_day)

## Write in the GOLD db, collection smart_contract_calls
result_1.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'tokens_count_by_day') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# ### 2. Group tokens by total token supply

tokens_by_supply_without_1 = "select query_token_types_final.total_supply token_supply, count(*) token_count \
        from query_token_types_final \
        where query_token_types_final.total_supply > 0 AND query_token_types_final.total_supply != 1 \
        group by query_token_types_final.total_supply \
        order by query_token_types_final.total_supply"

result_2 = spark.sql(tokens_by_supply)

## Write in the GOLD db, collection smart_contract_calls
result_2.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'tokens_by_supply_wo_1') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

tokens_by_supply_1_count = "select query_token_types_final.total_supply token_supply, count(*) tokens_count_tot_supply \
        from query_token_types_final \
        where query_token_types_final.total_supply > 0 AND query_token_types_final.total_supply = 1 \
        group by query_token_types_final.total_supply \
        order by query_token_types_final.total_supply"

result_3 = spark.sql(tokens_by_supply_1_count)

## Write in the GOLD db, collection smart_contract_calls
result_3.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'tokens_unique_supply') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# ### 3. NFTs minted in the network by day of year 

nfts_by_day = "select query_token_types_final.date_created, count(*) tokens_created_count \
                from query_token_types_final \
                where key = 721 \
                group by query_token_types_final.date_created"

result_4 = spark.sql(nfts_by_day)

## Write in the GOLD db, collection smart_contract_calls
result_4.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'nfts_count_by_day') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# ### 4. Fungible tokens minted in the network by day of year

fts_by_day = "select query_token_types_final.date_created, count(*) tokens_created_count \
                from query_token_types_final \
                where key NOT IN (721) \
                group by query_token_types_final.date_created"

result_5 = spark.sql(fts_by_day)

## Write in the GOLD db, collection smart_contract_calls
result_5.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'fts_count_by_day') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

spark.stop()

# +
# update the old checkpoints with new ones, based on current document count
multi_assets_query = { "collection": "multi_assets" }
new_multi_assets_count = { "$set": { "last_index": count_multi_assets } }

tx_query = { "collection": "tx" }
new_tx_count = { "$set": { "last_index": count_tx } }

ma_tx_mint_query = { "collection": "ma_tx_mint" }
new_ma_tx_mint_count = { "$set": { "last_index": count_ma_tx_mint } }

block_query = { "collection": "block" }
new_block_count = { "$set": { "last_index": count_block } }

tx_metadata_query = { "collection": "tx_metadata" }
new_tx_metadata_count = { "$set": { "last_index": count_tx_metadata } }

# update the last_ind table
last_ind.update_one(multi_assets_query, new_multi_assets_count)
last_ind.update_one(tx_query, new_tx_count)
last_ind.update_one(ma_tx_mint_query, new_ma_tx_mint_count)
last_ind.update_one(tx_metadata_query, new_tx_metadata_count)
