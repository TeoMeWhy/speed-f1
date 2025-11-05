# %%
import argparse

import os
import sys
import dotenv

import mlflow

dotenv.load_dotenv('../.env')

MLFLOW_SERVER = os.getenv("MLFLOW_SERVER")
MLFLOW_EXPERIMENT_ID = os.getenv("MLFLOW_EXPERIMENT_ID")
MLFLOW_CHAMP_MODEL = os.getenv("MLFLOW_CHAMP_MODEL")

mlflow.set_tracking_uri(MLFLOW_SERVER)

sys.path.append("..")
import spark_ops as spark_ops


# %%

def predict(spark, model, start, stop):
    df = (spark.table("fs_drivers")
               .filter(f"dtRef >= '{start}' AND dtRef < '{stop}'")
               .toPandas())

    predict = model.predict_proba(df[model.feature_names_in_])[:,1]
    df['predict'] = predict
    
    driver_cols = [
        'tempRoundNumber',
        'DriverId',
        'dtRef',
        'dtYear',
        'predict',
    ]
    return df[driver_cols]


def save_df_result(spark, df):
    
    (spark.createDataFrame(df)
          .createOrReplaceTempView("predict"))

    sql = """

        SELECT t1.*,
            t2.TeamName,
            '#' || t2.TeamColor AS TeamColor,
            t2.FullName

        FROM predict AS t1

        INNER JOIN results AS t2
        ON t1.tempRoundNumber = t2.RoundNumber
        AND t1.DriverId = t2.DriverId
        AND to_date(t1.dtRef) = to_date(t2.date)
        
        ORDER BY t2.date
    """
    
    sdf = spark.sql(sql)
    
    dates = ",".join([str(i) for i in sdf.toPandas()['dtYear'].astype(str).unique()])
    
    (sdf.coalesce(1)
        .write
        .mode("overwrite")
        .format('delta')
        .option("replaceWhere", f"dtYear in ({dates}) ")
        .partitionBy("dtYear")
        .save("data/gold/champ_prediction"))


def get_latest_model(model_name):
    register_model = mlflow.search_registered_models(filter_string=f"name='{model_name}'")[0]
    last_version = max([int(i.version) for i in register_model.latest_versions])
    model = mlflow.sklearn.load_model(f"models:///{model_name}/{last_version}")
    return model


def make_prediction(model_name, start, stop, spark):
    model = get_latest_model(model_name=model_name) 
    df = predict(spark, model, start, stop)
    save_df_result(spark, df)


def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=2024, type=int)
    parser.add_argument("--stop", default=2025, type=int)
    args = parser.parse_args()

    start = args.start
    stop = args.stop

    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("data/silver/fs_drivers", spark)
    spark_ops.create_view_from_path("data/bronze/results", spark)
    
    make_prediction(MLFLOW_CHAMP_MODEL, start, stop, spark)


if __name__ == "__main__":
    main()