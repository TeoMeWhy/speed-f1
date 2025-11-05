# %%
import os

import pandas as pd
from rich.progress import track

from delta import *

def consolidate_bronze(spark):

    for i in track(os.listdir("data/raw"), description="Convertendo dados..."):
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
        .save("data/bronze/results"))

def main():

    import sys
    sys.path.append(".")
    
    import spark_ops

    spark = spark_ops.new_spark_sesion()    
    consolidate_bronze(spark)


if __name__ == "__main__":
    main()