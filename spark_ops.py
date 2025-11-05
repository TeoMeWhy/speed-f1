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


def create_table(query, spark):
    table = query.split("/")[-1].split(".")[0]
    query = read_query(query)
    df = spark.sql(query)
    (df.coalesce(1)
       .write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"data/silver/{table}"))
    
    tableDelta = DeltaTable.forPath(spark, f"data/silver/{table}")
    tableDelta.vacuum()
    

class IngestorFS:
    
    def __init__(self, query, spark):
        self.table = query.split("/")[-1].split(".")[0]
        self.spark = spark
        self.query = read_query(query)

        
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