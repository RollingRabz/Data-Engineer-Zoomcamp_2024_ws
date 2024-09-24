# Batch processing with Spark

What is Batch?

    Batch is a processing a chunk of data at regular intervals.

* Example of batch job
    - Weekly
    - Daily
    - Hourly
    - Every x minutes

* Advantage of Batch
    - Easy to manage
        - Know what tool used in each step
    - Retry
        - If failed can retry
    - Scalability
        - New machine more spark clusters.

* disadvantage of Batch
    - Delay
        - If we have hourly job but the process time took 15 min so we wiil get data every 1 hour 15 minutes.


In this Module I run spark locally on my pc.

Run in terminal before jupyter notebook init

```bash
export JAVA_HOME="/c/tools/jdk-11.0.24"
export PATH="${JAVA_HOME}/bin:${PATH}"
java --version
export HADOOP_HOME="/c/tools/hadoop-3.2.0"
export PATH="${HADOOP_HOME}/bin:${PATH}"
export SPARK_HOME="/c/tools/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

Run

```bash
jupyter notebook
```

Run spark core 

```bash
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```