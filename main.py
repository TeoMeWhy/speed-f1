import logging
import time

logger = logging.getLogger("basic_logger")

import dotenv
dotenv.load_dotenv(".env")

from rich import print

from etl import raw, fs
from etl.bronze import consolidate_bronze
from etl.predict_champ import make_prediction as make_prediction_champ

import spark_ops



def run_raw(start, stop):
    print("Iniciando coleta de dados...")
    loader = raw.Loader(start, stop, ['race', 'sprint'])
    loader.process_start_stop()
    print("\nColeta finalizada.")
    

def run_bronze(spark):
    print("\n\nConsolidando dados em bronze...")
    consolidate_bronze(spark)
    print("\nCamada bronze criada.")


def run_silver(spark):
    
    print("Criando tabelas em silver...")
    spark_ops.create_view_from_path("data/bronze/results",spark)
    
    print("\tCriando tabela de campeões...")    
    spark_ops.create_table("etl/champions.sql", spark)
    
    print("\tCriando feature store...")
    fs.exec_range("etl/fs_drivers.sql", '2025-01-01', '2026-01-01', spark)
    
    print("Camada silver criada.")

    
def run_gold(spark):
    
    spark_ops.create_view_from_path("data/bronze/results",spark)
    spark_ops.create_view_from_path("data/silver/champions",spark)
    spark_ops.create_view_from_path("data/silver/fs_drivers",spark)
    
    print("Criando camada gold...")
    make_prediction_champ("f1_champion", '2000-01-01','2026-01-01', spark)
    
    print("Camada gold criada.")
    


def main():

    while True:
        logger.info("Execução iniciada")
        run_raw(2025,2025)

        spark = spark_ops.new_spark_sesion()
        run_bronze(spark)
        run_silver(spark)
        run_gold(spark)
        
        logger.info("Execução finalizada. Próxima execução em 24 horas.")
        time.sleep(60 * 60 * 24)
    
if __name__ == "__main__":
    main()