# %%
import argparse
import spark_ops 


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="", type=str)
    args = parser.parse_args()
    table = args.table
    
    if table == "":
        return
    
    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("data/bronze/results", spark)
    spark_ops.create_view_from_path("data/silver/fs_drivers", spark)

    try:
        spark_ops.create_view_from_path("data/silver/champions", spark)
        
    except Exception:
        spark_ops.create_table("champions", spark)
        
    spark_ops.create_table(table, spark)


if __name__ == "__main__":
    main()
    