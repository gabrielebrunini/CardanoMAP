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

# %% [markdown]
# - **On Notion**: find rewards for each pool/stake addresses
#
# Explanation of the resulting table:
# - id: index in the stake_address table
# - view: stake address, coming from the stake_address table
# - earned_epoch: The epoch in which the reward was earned, coming from the reward table
# - delegated_pool: The PoolHash table index for the pool the stake address was delegated to when the reward is earned or for the pool that there is a deposit refund. Coming from the reward table
# - lovelace: sum of the earned rewards, by stake_address. (ADA (₳) is the native token of Cardano. 1 ADA = 1,000,000 Lovelace).

# %% [markdown] tags=[]
# # Initial setup

# %%
import os
from pyspark.sql.functions import col, lit, when, from_json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import pyspark
from ast import literal_eval
from functools import reduce
from pyspark.sql.functions import col, lit, when, from_json
from graphframes import *
from delta import *
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import Row
from glob import glob
import networkx as nx
import pyvis
from pyvis.network import Network
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import *

# %%
# save Spark temporary files into folder /home/ubuntu/notebook/tmp_spark_files, with more space
config = pyspark.SparkConf().setAll([
    ('spark.driver.extraJavaOptions',
     '-Djava.io.tmpdir=/home/ubuntu/notebook/tmp_spark_files'),
    ('spark.executor.memory', '32g'), ('spark.executor.cores', '6'),
    ('spark.cores.max', '18'), ('spark.driver.memory', '32g'),
    ('spark.executor.instances', '3'),
    ('spark.worker.cleanup.enabled', 'true'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2'),
    ('spark.mongodb.output.writeConcern.wTimeoutMS', '120000'),
    ('spark.mongodb.output.writeConcern.socketTimeoutMS', '120000'),
    ('spark.mongodb.output.writeConcern.connectTimeoutMS', '120000')
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
#
# - treasury (?)

# %%
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
    order by earned_epoch asc;"

# %%
reward_history_address_result = spark.sql(reward_history_address)
reward_history_address_result.createOrReplaceTempView("reward_history_address")

# %%
## Write in the SILVER db, collection rewards_by_address. This is the first step of processing the data to find insights in the rewards history.
## Note: the writing takes place as temporary table on Spark
reward_history_address_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
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
# __1. Get the sum of distinct delegated pools in the network - count_delegated_pools__

# %%
count_delegated_pools = "select count(distinct reward_history_address.delegated_pool) \
from reward_history_address"

# %%
count_delegated_pools = spark.sql(count_delegated_pools)

# %%
count_delegated_pools.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'count_delegated_pools') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

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
