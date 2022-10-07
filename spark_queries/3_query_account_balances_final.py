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
#
# - [EXAMPLE ISSUE](https://github.com/input-output-hk/cardano-db-sync/issues/559), also explained in [Notion](https://www.notion.so/Account-balance-query-febd180af1834c5698ee062dc85589a7)
# - track account balance for a specific account
# - Procedure
#     - Addresses in the Shelley and later eras have two components; a payment credential and a staking credential. The stake address (eg stake1uyluup0rh6r2cc7kcw8nudqz990ezf5ltagxmw3u8deukvqwq7etq) is derived from the later. However it is possible to construct a valid address which does not contain any staking credential.
#     - What is inserted into the database is simply a reflection of what ledger state provides.
#     - The idea would be to have something like a pool_registration_refund table containing the stake address and the amount.

# %%
# %load_ext jupyternotify

# %% [markdown] tags=[]
# # Initial setup

# %%
import os
import pyspark
from pyspark.sql.functions import col, lit, when, from_json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StringType,IntegerType, ArrayType
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
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import  *
import pymongo
from pymongo import MongoClient

# %%
client = MongoClient("172.23.149.210", 27017)
db = client['cardano_silver']


inputs_table = db["inputs_table"]
outputs_table = db["outputs_table"]
rewards_table = db["rewards_table"]
withdrawals_table = db["withdrawals_table"]
get_stake_addr_table = db["get_stake_addr_table"]

# %%
# save Spark temporary files into folder /home/ubuntu/notebook/tmp_spark_files, with more space
config = pyspark.SparkConf().setAll([
    #('spark.driver.extraJavaOptions', '-Djava.io.tmpdir=/home/ubuntu/notebook/tmp_spark_files'),
    ('spark.executor.memory', '70g'),
    ('spark.executor.cores', '1'),
    ('spark.cores.max', '1'),
    ('spark.driver.memory','30g'),
    ('spark.executor.instances', '1'),
    ('spark.worker.cleanup.enabled', 'true'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2'),
    ('spark.mongodb.output.writeConcern.wTimeoutMS','120000'),
    ('spark.mongodb.output.writeConcern.socketTimeoutMS','120000'),
    ('spark.mongodb.output.writeConcern.connectTimeoutMS','120000')
])

# %% [markdown] tags=[]
# ### data stream

# %% tags=[]
spark = SparkSession \
    .builder \
	.config(conf=config) \
    .appName("MongoDB-account-balances_v2") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %% [markdown]
# ### mongodb imports

# %%
# slow to load: contains 73.8M rows!
tx_out = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.tx_out') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
tx_in = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.tx_in') \
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
tx = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.tx') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

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
withdrawal = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.withdrawal') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
tx_out.createOrReplaceTempView("tx_out")
tx_in.createOrReplaceTempView("tx_in")
stake_address.createOrReplaceTempView("stake_address")
tx.createOrReplaceTempView("tx")
withdrawal.createOrReplaceTempView("withdrawal")
reward.createOrReplaceTempView("reward")

# %%
addr_id = "select stake_address.view as stake_address,tx_out.stake_address_id \
    from tx_out inner join stake_address on tx_out.stake_address_id = stake_address.id"

# %%
spark.sql(addr_id).write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'addr_id') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.sql(addr_id).createOrReplaceTempView("addr_id")

# %%
inputs = "select stake_address_id, sum (value) value from tx_out group by stake_address_id;"

# %%
spark.sql(inputs).write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'inputs') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.sql(inputs).createOrReplaceTempView("inputs")

# %%
outputs = "select stake_address_id, sum (tx_out.value) as value \
    from tx_out inner join tx on tx_out.tx_id = tx.id \
    inner join tx_in on tx_in.tx_out_id = tx.id and tx_in.tx_out_index = tx_out.index \
    group by tx_out.stake_address_id;"

# %%
spark.sql(outputs).write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'outputs') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.sql(outputs).createOrReplaceTempView("outputs")

# %%
rewards = "select addr_id, sum (amount) amount from reward group by addr_id;"

# %%
spark.sql(rewards).write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'rewards') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.sql(rewards).createOrReplaceTempView("rewards")

# %%
withdrawals = "select addr_id, sum (amount) amount from withdrawal group by addr_id;"

# %%
spark.sql(withdrawals).write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'withdrawals') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.sql(withdrawals).createOrReplaceTempView("withdrawals")

# %% [raw]
# inputs = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'inputs') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()
# outputs = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'outputs') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()
# addr_id = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'addr_id') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()
# rewards = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'rewards') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()
# withdrawals = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'withdrawals') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# inputs.createOrReplaceTempView("inputs")
# addr_id.createOrReplaceTempView("addr_id")
# rewards.createOrReplaceTempView("rewards")
# withdrawals.createOrReplaceTempView("withdrawals")
# outputs.createOrReplaceTempView("outputs")

# %%
join = "select distinct addr_id.stake_address, \
inputs.stake_address_id input_stake_address, (inputs.value/1000000) input_value, \
(outputs.value/1000000) output_value, \
(rewards.amount/1000000) rewards_value, \
(withdrawals.amount/1000000) withdrawals_value \
from inputs \
left join addr_id on inputs.stake_address_id = addr_id.stake_address_id \
full join outputs on inputs.stake_address_id = outputs.stake_address_id \
full join rewards on inputs.stake_address_id = rewards.addr_id \
full join withdrawals on inputs.stake_address_id = withdrawals.addr_id"

# %%
accnt_balance = spark.sql(join).na.fill(0).withColumn("account_balance_ADA", 
                               (col("input_value")+col("rewards_value")-col("output_value")-col("withdrawals_value")
                               )).orderBy("account_balance_ADA", ascending=False)

# %%
accnt_balance.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'accnt_balance') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.stop()

# %% [raw] tags=[]
# # Query: retrieve account balances (descending order)
# #### Balance calculation for a given stake address: inputs + rewards - outputs - withdrawals

# %% [raw]
# inputs = "select stake_address_id, sum (value)/1000000 from tx_out group by stake_address_id"

# %% [raw]
# outputs = "select stake_address_id, sum (tx_out.value)/1000000 as output_value \
#     from tx_out \
#     inner join tx on tx_out.tx_id = tx.id \
#     inner join tx_in on tx_in.tx_out_id = tx.id and tx_in.tx_out_index = tx_out.index \
#     group by stake_address_id"

# %% [raw]
# # include the table coming from query 2 - rewards by address
# rewards = "select reward.addr_id, sum(reward.amount) as reward_amount \
# from reward \
# group by reward.addr_id"

# %% [raw]
# withdrawals = "select addr_id, sum(amount)/1000000 as ADA_withdrawal from withdrawal \
# group by addr_id"

# %% [raw]
# get_stake_addr = "select stake_address.id as stake_address_id, tx_out.address, stake_address.view as stake_address \
#     from tx_out \
#     full join stake_address on tx_out.stake_address_id = stake_address.id"

# %% [raw]
# show the resulting tables

# %% [raw]
# create the temporary tables in spark

# %% [raw]
# inputs_table.drop()
# outputs_table.drop()
# rewards_table.drop()
# withdrawals_table.drop()
# get_stake_addr_table.drop()

# %% [raw]
# spark.sql(inputs).write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'inputs_table') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# spark.sql(outputs).write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'outputs_table') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# spark.sql(rewards).write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'rewards_table') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# spark.sql(withdrawals).write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'withdrawals_table') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# spark.sql(get_stake_addr).write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'get_stake_addr_table') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# inputs = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'inputs_table') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# outputs = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'outputs_table') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# rewards = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'rewards_table') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# withdrawals = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'withdrawals_table') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# get_stake_addr = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'get_stake_addr_table') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# inputs.createOrReplaceTempView("inputs_table")
# outputs.createOrReplaceTempView("outputs_table")
# rewards.createOrReplaceTempView("rewards_table")
# withdrawals.createOrReplaceTempView("withdrawals_table")
# get_stake_addr.createOrReplaceTempView("get_stake_addr_table")

# %% [raw]
# spark.sql(inputs).createOrReplaceTempView("inputs_table")

# %% [raw]
# spark.sql(outputs).createOrReplaceTempView("outputs_table")

# %% [raw]
# spark.sql(rewards).createOrReplaceTempView("rewards_table")

# %% [raw]
# spark.sql(withdrawals).createOrReplaceTempView("withdrawals_table")

# %% [raw]
# spark.sql(get_stake_addr).createOrReplaceTempView("get_stake_addr_table")

# %% [raw]
# Join the tables together - full join: even if one or more of the addresses on the tables are missing, the addresses still need to be reported in the final result. If one does an inner join of the tables, then only the records that correspond 1 to 1 in every table of the joined ones are reported, therefore, in case one or more of the values are missing, the whole account is not reported in the final report.

# %% [raw]
# final = "select inputs_table.stake_address_id,accnt_address, (inputs_table.input_value/1000000) input_value, \
#         (outputs_table.output_value/1000000) output_value, \
#         (rewards_table.rewards_value/1000000) rewards_value, \
#         (withdrawals_table.withdrawals_value/1000000) withdrawals_value, \
#         (reserve_table.reserve_value/1000000) reserve_value \
#         from inputs_table \
#         full join outputs_table on inputs_table.stake_address_id = outputs_table.stake_address_id \
#         full join rewards_table on inputs_table.stake_address_id = rewards_table.addr_id \
#         full join withdrawals_table on inputs_table.stake_address_id = withdrawals_table.addr_id \
#         full join reserve_table on inputs_table.stake_address_id = reserve_table.addr_id"

# %% [raw]
# final = "select \
#         get_stake_addr_table.address, \
#         get_stake_addr_table.stake_address, \
#         sum((inputs_table.input_value/1000000)) input_value, \
#         sum((outputs_table.output_value/1000000)) output_value, \
#         sum((rewards_table.rewards_value/1000000)) rewards_value, \
#         sum((withdrawals_table.withdrawals_value/1000000)) withdrawals_value \
#         from inputs_table \
#         full join outputs_table on inputs_table.stake_address_id = outputs_table.stake_address_id \
#         full join rewards_table on inputs_table.stake_address_id = rewards_table.addr_id \
#         full join withdrawals_table on inputs_table.stake_address_id = withdrawals_table.addr_id \
#         left join get_stake_addr_table on inputs_table.stake_address_id = get_stake_addr_table.stake_address_id \
#         group by get_stake_addr_table.address, get_stake_addr_table.stake_address"

# %% [raw]
# spark.sql(final).show()

# %% [raw]
# spark.sql(final).createOrReplaceTempView("final_table")

# %% [raw]
# final_stake_address = "select get_stake_addr_table.stake_address, final_table.* \
# from final_table \
# inner join get_stake_addr_table on get_stake_addr_table.stake_address_id = final_table.stake_address_id"

# %% [raw]
# The final result filters out the stake addresses that are reported multiple times in the tables, so that only one document per stake address is present in the final result.

# %% [raw]
# final_stake_address = "select distinct get_stake_addr_table.stake_address, final_table.* \
# from final_table \
# inner join get_stake_addr_table on get_stake_addr_table.stake_address_id = final_table.stake_address_id"

# %% [raw]
# spark.sql(final_stake_address).show()

# %% [raw]
#  inputs + rewards - outputs- withdrawals
#  - Fill-in with zeros all the numeric columns which have NA value

# %% [raw]
# accnt_balance = spark.sql(final_stake_address).na.fill(0).withColumn("account_balance_ADA", 
#                                (col("input_value")+col("rewards_value")-col("output_value")-col("withdrawals_value")-col("reserve_value")
#                                ))

# %% [raw]
# accnt_balance = spark.sql(final).na.fill(0).withColumn("account_balance_ADA", 
#                                (col("input_value")+col("rewards_value")-col("output_value")-col("withdrawals_value")
#                                )).orderBy("account_balance_ADA", ascending=False)

# %% [raw]
# accnt_balance_table.drop()

# %% [raw]
# accnt_balance.write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'account_balance') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# accnt_balance = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_silver') \
#   	.option('spark.mongodb.collection', 'account_balance') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# accnt_balance.createOrReplaceTempView("accnt_balance")

# %% [raw]
# spark.stop()

# %% [raw]
# ### Whales vs network

# %% [raw]
# whales_table.drop()

# %% [raw]
# spark = SparkSession \
#     .builder \
# 	.config(conf=config) \
#     .appName("MongoDB-account-balances_v2") \
#     .master("spark://172.23.149.210:7077") \
#     .getOrCreate()

# %% [raw]
# accnt_balance = spark.read.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.option('spark.mongodb.database', 'cardano_gold') \
#   	.option('spark.mongodb.collection', 'account_balance') \
# 	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
# 	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.load()

# %% [raw]
# tx_out.createOrReplaceTempView("tx_out")

# %% [raw]
# top_10 = "select accnt_balance.account_balance_ADA,accnt_address \
#                 from accnt_balance \
#                 order by account_balance_ADA desc \
#                 limit 10"

# %% [raw]
# spark.sql(top_10).show()

# %% [raw]
# # Top 10 account balances sum
# top_10_whales = "SELECT SUM(account_balance_ADA) \
#                 from (select accnt_balance.account_balance_ADA \
#                 from accnt_balance \
#                 order by account_balance_ADA desc \
#                 limit 10 )"

# %% [raw]
# top_10_whales_result = spark.sql(top_10_whales)

# %% [raw]
# top_10_whales_result.write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_gold') \
#   	.option('spark.mongodb.collection', 'top_10_whales_ADA') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# balance_tot_sum = "select sum(account_balance_ADA)\
# from accnt_balance"

# %% [raw]
# balance_tot_sum_result = spark.sql(balance_tot_sum)

# %% [raw]
# balance_tot_sum_result.write.format("mongodb") \
# 	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
#   	.mode("overwrite") \
#     .option('spark.mongodb.database', 'cardano_gold') \
#   	.option('spark.mongodb.collection', 'tot_account_balance') \
#   	.option("forceDeleteTempCheckpointLocation", "true") \
#   	.save()

# %% [raw]
# spark.stop()
