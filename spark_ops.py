from tqdm import tqdm

import pyspark
from delta import *

from rich.progress import track


def read_query(path):
    with open(path) as open_file:
        return open_file.read()


def new_spark_sesion():
        builder = (pyspark.sql
                          .SparkSession
                          .builder
                          .appName("MyApp")
                          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark


def create_view_from_path(path, spark):
    df = spark.read.format("delta").load(path)
    table = path.split("/")[-1]
    df.createOrReplaceTempView(table)


def create_table(table, spark):
    query = read_query(f"{table}.sql")
    df = spark.sql(query)
    (df.coalesce(1)
       .write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"data/silver/{table}"))
    


class IngestorFS:
    
    def __init__(self, table, spark):
        self.table = table
        self.spark = spark
        self.query = read_query(f"{table}.sql")

        
    def load(self, date):
        query = self.query.format(date=date)
        return self.spark.sql(query)

         
    def save(self, df, date):
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("replaceWhere", f"dtRef = '{date}' ")
           .partitionBy("dtYear")
           .save(f"data/silver/{self.table}"))


    def exec(self, iters):
        for i in track(iters, description="Executando..."):
            df = self.load(i)
            self.save(df, i)
            
        (self.spark
             .read
             .format("delta")
             .load(f"data/silver/{self.table}")
             .coalesce(1)
             .write
             .format("delta")
             .mode("overwrite")
             .partitionBy("dtYear")
             .save(f"data/silver/{self.table}"))

        tableDelta = DeltaTable.forPath(self.spark, f"data/silver/{self.table}")
        tableDelta.vacuum()