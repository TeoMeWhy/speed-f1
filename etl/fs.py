# %%
import argparse

import sys

from pyspark.sql import functions as F
from rich.console import Console
console = Console()

sys.path.append(".")
import spark_ops

def exec_range(query, start, stop, spark):
    iters = (spark.table("results")
                  .filter(f"to_date(date) >= '{start}' and to_date(date) <= '{stop}'")
                  .select(F.to_date("date"))
                  .distinct()
                  .orderBy('to_date(date)')
                  .toPandas()['to_date(date)']
                  .astype(str)
                  .tolist())
    
    ingest = spark_ops.IngestorFS(query, spark)
    ingest.exec(iters)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default="", type=str)
    parser.add_argument("--start", default="2025-01-01", type=str)
    parser.add_argument("--stop", default="2026-01-01", type=str)
    args = parser.parse_args()
    query = args.query
    
    if query == "":
        return
    
    start, stop = args.start, args.stop
    spark = spark_ops.new_spark_sesion()
    
    spark_ops.create_view_from_path("data/bronze/results",spark)
    exec_range(query, start, stop, spark)

    
# %%
if __name__ == "__main__":
    main()
    