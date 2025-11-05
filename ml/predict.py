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

sys.path.append("../")

import spark_ops
# %%

def predict(spark, model, start, stop):
    df = (spark.table("fs_drivers")
               .filter(f"dtRef >= '{start}-01-01' AND dtRef < '{stop+1}-01-01'")
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


def save_df_result(spark, df, dates):
    
    (spark.createDataFrame(df)
          .createOrReplaceTempView("predict"))

    sql = """

        SELECT t1.*,
            t2.TeamName,
            t2.TeamColor,
            t2.FullName

        FROM predict AS t1

        INNER JOIN results AS t2
        ON t1.tempRoundNumber = t2.RoundNumber
        AND t1.DriverId = t2.DriverId
        AND to_date(t1.dtRef) =    
 to_date(t2.date)
    """

    (spark.sql(sql)
        .coalesce(1)
        .write
        .mode("overwrite")
        .format('delta')
        .option("replaceWhere", f"dtYear in ({dates}) ")
        .partitionBy("dtYear")
        .save("../data/gold/champ_prediction")
    )


def get_latest_model(model_name):
    register_model = mlflow.search_registered_models(filter_string=f"name='{model_name}'")[0]
    last_version = max([int(i.version) for i in register_model.latest_versions])
    model = mlflow.sklearn.load_model(f"models:///{model_name}/{last_version}")
    return model


def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=2024, type=int)
    parser.add_argument("--stop", default=2025, type=int)
    args = parser.parse_args()
    
    model = get_latest_model(MLFLOW_CHAMP_MODEL)
    
    start = args.start
    stop = args.stop

    dates = [str(i) for i in range(start, stop+1)]
    str_dates = ",".join(dates)

    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("../data/silver/fs_drivers", spark)
    spark_ops.create_view_from_path("../data/bronze/results", spark)

    df = predict(spark, model, start, stop)
    save_df_result(spark, df, str_dates)


if __name__ == "__main__":
    main()