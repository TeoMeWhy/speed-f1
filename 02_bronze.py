# %%
import os
import pandas as pd
from tqdm import tqdm 

import pyspark
from delta import *

builder = (pyspark.sql.SparkSession.builder.appName("MyApp")
                  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()


for i in tqdm(os.listdir("data/raw")):
    if i.endswith(".parquet"):
        path = f"data/raw/{i}"
        df = pd.read_parquet(path)
        df.to_csv(path.replace(".parquet", ".csv"), sep=';', index=False)
    

df = (spark.read
           .csv("data/raw/*.csv",sep=";", header=True))

(df.coalesce(1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("data/bronze/results")
    )
