# %%
import argparse
import spark_ops

from pyspark.sql import functions as F

from rich.console import Console

console = Console()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="", type=str)
    parser.add_argument("--start", default="2025-01-01", type=str)
    parser.add_argument("--stop", default="2025-01-01", type=str)
    args = parser.parse_args()
    table = args.table
    
    if table == "":
        return
    
    start, stop = args.start, args.stop
    
    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("data/bronze/results", spark)
    
    iters = (spark.table("results")
                  .filter(f"to_date(date) >= '{start}' and to_date(date) <= '{stop}'")
                  .select(F.to_date("date"))
                  .distinct()
                  .orderBy('to_date(date)')
                  .toPandas()['to_date(date)']
                  .astype(str)
                  .tolist())
    
    ingest = spark_ops.IngestorFS(table, spark)
    ingest.exec(iters)

# %%
if __name__ == "__main__":
    main()
    