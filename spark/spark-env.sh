SPARK_MASTER_HOST=172.23.149.210
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=28080
SPARK_WORKER_WEBUI_PORT=28081
SPARK_WORKERS_OPTS="spark.worker.cleanup.enabled=true"
SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -D spark.worker.cleanup.enabled=true
        -D spark.executor.logs.rolling.strategy=time \
        -D spark.executor.logs.rolling.time.interval=hourly \
        -D spark.executor.logs.rolling.maxRetainedFiles=3"
