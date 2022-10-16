# Spark Configuration Files

Before starting up those nodes, writing a configuration file for the Spark environment is recommended. We defined this file by default as spark-env.sh, and it is located by default in the folder /SPARK\_HOME/conf of the Druid machine. In our specific case, the assigned HOME Spark directory is /opt/spark.

The configuration file, named spark-defaults.conf, is placed in /SPARK\_HOME/conf. 
- The number of cores, memory utilization for apps, and the ability to enable dynamic allocation and rolling logs can define all worker cleanup in this configuration. Several more settings are available in the Spark documentation, but we decided to leave them as default for our Spark applications.

We can separate Spark properties into two kinds: one is related to deployment, and just like spark.driver.memory, spark.executor.instances, this kind of property may not be affected when setting programmatically through SparkConf in runtime, or the behaviour depends on which cluster manager and deploys mode you choose. We suggest to set through a configuration file or spark-submit command line options. Another mainly relates to Spark runtime control, like spark.task.maxFailures. One can set this kind of property either way.
