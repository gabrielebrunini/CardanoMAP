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
import time

# %%
# Grab Currrent Time Before Running the Code
start = time.time()

# %%
client = MongoClient("172.23.149.210", 27017)
db = client['cardano_bronze']

# %%
# Import the needed tables
reward = db["node.public.reward"]

last_ind = db["last_indexes_2_rewards_history"]

# import required temporary collections to overwrite with new data
reward_tmp = db["reward_temporary"]

# %%
# %%
# insert initial values in checkpoint table
#initial_val_reward = { "collection": "reward", "last_index": 0 }
#initial_val_pool_hash = { "collection": "pool_hash", "last_index": 0 }
#initial_val_stake_address = { "collection": "stake_address", "last_index": 0 }
#
#last_ind.insert_one(initial_val_reward)
#last_ind.insert_one(initial_val_pool_hash)
#last_ind.insert_one(initial_val_stake_address)

# %%
#retrieve the last indices that were processed before
reward_last_processed = last_ind.find_one({'collection': 'reward'})['last_index']

# %%
# count how many documents are in each new input mongodb collection
count_reward = reward.estimated_document_count()

# %%
# for each Cardano table, select the records which haven't been processed yet (range between last_processed and total records count)
reward_df = reward.find()[reward_last_processed:count_reward]

# %%
# drop the previous records in the temporary collections
reward_tmp.drop()

# %%
# load the temporary records in the temporary collections
reward_tmp.insert_many(reward_df)

# %% [markdown] tags=[]
# # Initial setup

# %%
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
    .appName("MongoDB-rewards-history") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %% [markdown] tags=[]
# # Tables needed

# %% [markdown]
# - reward
# - pool_hash
# - stake_address

# %%
reward = spark.read.format("mongodb") \
 .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'reward_temporary') \
 .option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
 .option('spark.mongodb.change.stream.publish.full.document.only','true') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .load()

# %% tags=[]
pool_hash = spark.read.format("mongodb") \
 .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'node.public.pool_hash') \
 .option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
 .option('spark.mongodb.change.stream.publish.full.document.only','true') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .load()

# %%
stake_address = spark.read.format("mongodb") \
 .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'node.public.stake_address') \
 .option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
 .option('spark.mongodb.change.stream.publish.full.document.only','true') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .load()

# %%
reward.createOrReplaceTempView("reward")
pool_hash.createOrReplaceTempView("pool_hash")
stake_address.createOrReplaceTempView("stake_address")

# %% [markdown]
# # Get the reward history per address:
# Meaning: retrieve the rewards by epoch, grouping by address. Delegated pools are also reported in the query.

# %%
reward_history_address = "select stake_address.id, stake_address.view, reward.earned_epoch, \
pool_hash.view as delegated_pool, reward.amount as lovelace \
    from reward \
    inner join stake_address on reward.addr_id = stake_address.id \
    inner join pool_hash on reward.pool_id = pool_hash.id \
    order by earned_epoch desc;"

# %%
reward_history_address_result = spark.sql(reward_history_address)
reward_history_address_result.createOrReplaceTempView("reward_history_address")

# %%
## Write in the SILVER db, collection rewards_by_address. This is the first step of processing the data to find insights in the rewards history.
## Note: the writing takes place as temporary table on Spark
reward_history_address_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("append") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'rewards_history') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %% [markdown]
# # Intermediate import of rewards_history table

# %%
reward_history_address = spark.read.format("mongodb") \
 .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .option('spark.mongodb.database', 'cardano_silver') \
   .option('spark.mongodb.collection', 'rewards_history') \
 .option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
 .option('spark.mongodb.change.stream.publish.full.document.only','true') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .load()

# %%
reward_history_address.createOrReplaceTempView("reward_history_address")

# %% [markdown]
# ### Gold database results

# %% [markdown]
# __2. Get the total rewards by epoch - rewards_by_epoch__

# %%
rewards_by_epoch = "select earned_epoch, sum(lovelace)/1000000 as tot_ADA \
from reward_history_address \
group by earned_epoch"

# %%
rewards_by_epoch = spark.sql(rewards_by_epoch)

# %%
rewards_by_epoch.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'rewards_by_epoch') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %% [markdown]
# __3. Get the total rewards by pool - rewards_by_pool__

# %%
rewards_by_pool = "select delegated_pool, sum(lovelace)/1000000 as tot_ADA \
from reward_history_address \
group by delegated_pool"

# %%
rewards_by_pool = spark.sql(rewards_by_pool)

# %%
rewards_by_pool.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'rewards_by_pool') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %% [markdown]
# __4. Proportion of top stakers - distribution_whales__

# %%
distribution_whales = "select view, sum(lovelace)/1000000 as tot_ADA \
from reward_history_address \
group by view \
order by tot_ADA desc \
limit 10"

# %%
distribution_whales = spark.sql(distribution_whales)

# %%
distribution_whales.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'distribution_whales') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
# the full REWARD table is now required to compute the rewards by type and sum of ADA rewards by type subqueries
reward = spark.read.format("mongodb") \
 .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
   .option('spark.mongodb.database', 'cardano_bronze') \
   .option('spark.mongodb.collection', 'node.public.reward') \
 .option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
 .option('spark.mongodb.change.stream.publish.full.document.only','true') \
   .option("forceDeleteTempCheckpointLocation", "true") \
   .load()

# %%
reward.createOrReplaceTempView("reward")

# %% [markdown]
# __5. Count how many rewards by type__

# %%
rewards_by_type = "select count(*),type from reward group by type"

# %%
rewards_by_type = spark.sql(rewards_by_type)

# %%
rewards_by_type.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'rewards_by_type') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %% [markdown]
# __6. Sum ADA rewards by type__

# %%
sum_rewards_by_type = "select sum(amount)/1000000 as ADA_amount,type from reward group by type"

# %%
sum_rewards_by_type = spark.sql(sum_rewards_by_type)

# %%
sum_rewards_by_type.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'ADA_rewards_by_type') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.stop()

# %%
# update the old checkpoints with new ones, based on current document count
reward_query = { "collection": "reward" }
new_reward_count = { "$set": { "last_index": count_reward } }

last_ind.update_one(reward_query, new_reward_count)

# %%
# Grab Currrent Time After Running the Code
end = time.time()

#Subtract Start Time from The End Time
total_time = end - start
print("\n"+ str(total_time))
