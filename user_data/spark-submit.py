#!/usr/local/bin/python3

import os
os.environ["JAVA_HOME"] = "/usr/local/openjdk-8"
os.environ["SPARK_HOME"] = "/user_data/spark-3.3.0-bin-hadoop2"

import findspark
findspark.init('spark-3.3.0-bin-hadoop2')

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("sparksubmit_test_app")
    .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
    .config("spark.sql.catalogImplementation", "hive")
    .getOrCreate()
)

df = spark.read.text("hdfs://spark-master:9000/datasets/")

df.show()

import requests
from io import StringIO

df = spark.read.csv("hdfs://spark-master:9000/datasets/iris/iris.data", header=False, inferSchema=True)

df.show()

num_linhas = df.count()
print(f"Número de linhas no DataFrame: {num_linhas}")

df_transformado = df.select("_c0", "_c1").filter(df["_c4"] == 'Iris-setosa')

df_transformado.show()

# Registrar DataFrame como tabela temporária
df.createOrReplaceTempView("tabela_temporaria")

# Executar consulta SQL no DataFrame
resultado_sql = spark.sql("SELECT _c0, AVG(_c1) FROM tabela_temporaria GROUP BY _c0")
resultado_sql.show()