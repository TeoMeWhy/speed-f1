import streamlit as st
import sys

import matplotlib.pyplot as plt
import seaborn as sns

from colores import *

sns.set_style()

sys.path.append("..")

import spark_ops as spark_ops

@st.cache_resource(ttl=60*10)
def get_data():
    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("../data/gold/champ_prediction", spark)
    df = spark.table("champ_prediction").toPandas()
    
    df['YearRound'] = df.apply(lambda row: f"{row['dtYear']}-{int(row['tempRoundNumber']):02d}", axis=1)
    return df
    

def show_data(df):
    
    columns_config = {
        "dtYear":st.column_config.NumberColumn("Ano Temporada",help="Ano que a temporada está ocorrendo"),
        "tempRoundNumber": st.column_config.NumberColumn("Rodada",help="Rodada do campeonato"),
        "dtRef": st.column_config.TextColumn("Data Predição",help="Data em que a predição ocorreu, pós corrida. Seja sprint ou grande prêmio"),
        "FullName": st.column_config.TextColumn("Piloto",help="Nome do Piloto"),
        "DriverId": st.column_config.TextColumn("ID Piloto",help="Identificação do piloto"),
        "TeamName": st.column_config.TextColumn("Equipe",help="Nome da Equipe"),
        "TeamColor": st.column_config.TextColumn("Cor Equipe",help="Cor adotada pela equipe"),
        "predict":st.column_config.NumberColumn("Probabilidade",help="Probabilidade de ser campeão na temporada", format='percent'),
    }
    
    columns_order = [
        "dtYear",
        "tempRoundNumber",
        "dtRef",
        "FullName",
        "DriverId",
        "TeamName",
        "TeamColor",
        "predict",
    ]
    
    # df = df.drop(['DriverId', "TeamColor"], axis=1)
    st.dataframe(df, column_order=columns_order, column_config=columns_config, hide_index=True)

st.set_page_config(
    page_title="Speed F1",
    page_icon=":racing_car:",
    layout='wide',
)

st.markdown("""
# Speed F1
"""
)

t1, t2 = st.columns(2)

t1.markdown("""
## Boas vindas!

Projeto de coleta, procesamento e criação de aplicações de dados da F1 realizado ao vivo.

[:link: Gravação disponível no YouTube](https://www.youtube.com/playlist?list=PLvlkVRRKOYFRha5ExLDyf7jbOVII55JRH)

[:link: Repositório completo do projeto](https://github.com/TeoMeWhy/speed-f1).

## Objetivo
            
Modelo que atribui probabilidade para cada piloto ser campeão mundial.


""")

t2.markdown("""

## Etapas

- Coleta de dados de grande prêmios e sprints de 1990 a 2025;
- Processamento em Apache Spark para consolidação;
- Criação de Feature Store para piloto ao final de cada corrida (GP e Sprint);
- Treino do modelo de ML com dados entre 1990 a 2023;
- Teste do modelo em 2024;
- Aplicação do modelo no campeonato de 2025;

""")


df_champ = get_data()

names_default = (df_champ[df_champ["YearRound"]==df_champ["YearRound"].max()]
                        .sort_values(by='predict',ascending=False)['FullName']
                        .head(3)
                        .tolist()
                        )

if st.checkbox("Mostrar dados"):
    show_data(df_champ)

col1, col2 = st.columns([1,3])
year = col1.number_input(label="Ano Temporada",
                       min_value=df_champ["dtYear"].min(),
                       max_value=df_champ["dtYear"].max(),
                       value=df_champ["dtYear"].max())

names = col2.multiselect(label="Pilotos", options=df_champ['FullName'].unique(), default=names_default)


df = df_champ[df_champ['dtYear']==year]
df = df[df['FullName'].isin(names)]

df.to_csv("df.csv")

df_pivot = df.pivot_table(index='YearRound', columns='FullName', values='predict').reset_index()

colors = get_colors(df)


st.line_chart(df_pivot,
              x='YearRound',
              y=df_pivot.columns.tolist()[1:],
              color=colors,
              x_label="Temporada/Rodada",
              y_label="Probabilidade")