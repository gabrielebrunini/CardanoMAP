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
import time
start = time.time()
# %%
# save Spark temporary files into folder /home/ubuntu/notebook/tmp_spark_files, with more space
config = pyspark.SparkConf().setAll([
    #('spark.driver.extraJavaOptions', '-Djava.io.tmpdir=/home/ubuntu/notebook/tmp_spark_files'),
    ('spark.executor.memory', '70g'),
    ('spark.executor.cores', '3'),
    ('spark.cores.max', '12'),
    ('spark.driver.memory','30g'),
    ('spark.executor.instances', '4'),
    ('spark.worker.cleanup.enabled', 'true'),
    ('spark.worker.cleanup.interval', '60'),
    ('spark.worker.cleanup.appDataTtl', '60'),
    ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2'),
    ('spark.mongodb.output.writeConcern.wTimeoutMS','120000'),
    ('spark.mongodb.output.writeConcern.socketTimeoutMS','120000'),
    ('spark.mongodb.output.writeConcern.connectTimeoutMS','120000')
])

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

# %% [raw]
# inputs_table.drop()
# outputs_table.drop()
# rewards_table.drop()
# withdrawals_table.drop()
# get_stake_addr_table.drop()

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

# %%
spark.stop()

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

# %%
spark = SparkSession \
    .builder \
	.config(conf=config) \
    .appName("MongoDB-account-balances_v2") \
    .master("spark://172.23.149.210:7077") \
    .getOrCreate()

# %%
inputs = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'inputs') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()
outputs = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'outputs') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

rewards = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'rewards') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()
withdrawals = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'withdrawals') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()
stake_address = spark.read.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.option('spark.mongodb.database', 'cardano_bronze') \
  	.option('spark.mongodb.collection', 'node.public.stake_address') \
	.option('spark.mongodb.read.readPreference.name', 'primaryPreferred') \
	.option('spark.mongodb.change.stream.publish.full.document.only','true') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.load()

# %%
inputs.createOrReplaceTempView("inputs")
rewards.createOrReplaceTempView("rewards")
withdrawals.createOrReplaceTempView("withdrawals")
outputs.createOrReplaceTempView("outputs")
stake_address.createOrReplaceTempView("stake_address")

# %% [markdown]
# Join the tables together - full join: even if one or more of the addresses on the tables are missing, the addresses still need to be reported in the final result. If one does an inner join of the tables, then only the records that correspond 1 to 1 in every table of the joined ones are reported, therefore, in case one or more of the values are missing, the whole account is not reported in the final report.

# %% [markdown]
# The final result filters out the stake addresses that are reported multiple times in the tables, so that only one document per stake address is present in the final result.

# %%
join = "select distinct inputs.stake_address_id input_stake_address, \
stake_address.view, \
(inputs.value/1000000) input_value, \
(outputs.value/1000000) output_value, \
(rewards.amount/1000000) rewards_value, \
(withdrawals.amount/1000000) withdrawals_value \
from inputs \
full join outputs on inputs.stake_address_id = outputs.stake_address_id \
full join rewards on inputs.stake_address_id = rewards.addr_id \
full join withdrawals on inputs.stake_address_id = withdrawals.addr_id \
left join stake_address on inputs.stake_address_id = stake_address.id \
where inputs.stake_address_id <> 0"

# %%
accnt_balance = spark.sql(join).na.fill(0).withColumn("account_balance_ADA", 
                               (col("input_value")+col("rewards_value")-col("output_value")-col("withdrawals_value")
                               )).orderBy("account_balance_ADA", ascending=False).select(col("input_stake_address"),col("view"), col("account_balance_ADA"))

# %%
accnt_balance.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_silver') \
  	.option('spark.mongodb.collection', 'accnt_balance') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
accnt_balance.createOrReplaceTempView("accnt_balance")

# %%
top_10 = "select accnt_balance.view, accnt_balance.account_balance_ADA \
                from accnt_balance \
                order by account_balance_ADA desc \
                limit 10"

# %%
top_10_whales_result = spark.sql(top_10)

# %%
top_10_whales_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'top_10_whales_ADA') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
# Top 10 account balances sum
top_10_sum = "SELECT SUM(account_balance_ADA) \
                from (select accnt_balance.account_balance_ADA \
                from accnt_balance \
                order by account_balance_ADA desc \
                limit 10 )"

# %%
top_10_sum = spark.sql(top_10_sum)

# %%
top_10_sum.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'top_10_sum') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
balance_tot_sum = "select sum(account_balance_ADA)\
from accnt_balance"

# %%
balance_tot_sum_result = spark.sql(balance_tot_sum)

# %%
balance_tot_sum_result.write.format("mongodb") \
	.option('spark.mongodb.connection.uri', 'mongodb://172.23.149.210:27017') \
  	.mode("overwrite") \
    .option('spark.mongodb.database', 'cardano_gold') \
  	.option('spark.mongodb.collection', 'tot_account_balance') \
  	.option("forceDeleteTempCheckpointLocation", "true") \
  	.save()

# %%
spark.stop()

# Grab Currrent Time After Running the Code
end = time.time()

#Subtract Start Time from The End Time
total_time = end - start
print("\n"+ str(total_time))
