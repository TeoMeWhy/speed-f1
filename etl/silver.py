# %%
import argparse
import sys

sys.path.append(".")
import spark_ops


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default="", type=str)
    args = parser.parse_args()
    query = args.query
    
    if query == "":
        return
    
    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("data/bronze/results", spark)
    spark_ops.create_view_from_path("data/silver/fs_drivers", spark)

    try:
        spark_ops.create_view_from_path("data/silver/champions", spark)
        
    except Exception:
        spark_ops.create_table("etl/champions.sql", spark)
        
    spark_ops.create_table(query, spark)


if __name__ == "__main__":
    main()
    