# %%

import sys
import os

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme()

from feature_engine import imputation
from sklearn import pipeline
from sklearn import ensemble
from sklearn import metrics

import mlflow
import dotenv

dotenv.load_dotenv('../.env')

MLFLOW_SERVER = os.getenv("MLFLOW_SERVER")
MLFLOW_EXPERIMENT_ID = os.getenv("MLFLOW_EXPERIMENT_ID")

print(MLFLOW_SERVER)
print(MLFLOW_EXPERIMENT_ID)

#%%

mlflow.set_tracking_uri(MLFLOW_SERVER)
mlflow.set_experiment(experiment_id= MLFLOW_EXPERIMENT_ID)


# %%
sys.path.append("..")

import spark_ops as spark_ops

# %%

# Busca pelos dados da ABT no spark
spark = spark_ops.new_spark_sesion()
spark_ops.create_view_from_path("../data/silver/abt_champ", spark)

# Trás os dados para Pandas
df = spark.table("abt_champ").toPandas()
df['tempRoundNumber'] = df['tempRoundNumber'].astype(int)

df.head()

# %%

# Filtra o dado para Out Of Time, deixando o camp de 2024 fora do treino do ML
df_oot = df[df['dtYear']==2024]

# Definie base de treino com dados anteriores a 2024
df_train = df[df['dtYear']<2024]

# %%

# Seleciona apenas as colunas de piloto e ano pegando distintos
df_sample = df_train[['DriverId', "dtYear"]].drop_duplicates()

# gera número aleatório para cada linha
df_sample['random'] = np.random.uniform(size=df_sample.shape[0])

# marca como treino quem teve número aleatório maior que 0.2
df_sample['train'] = df_sample['random'] >= 0.2

# marca como teste quem teve número aleatório menos que 0.2
df_sample['test'] = ~df_sample['train']

# %%

# seleciona apenas as linhas marcadas como treino e as colunas de piloto e ano
df_sample_train = df_sample[df_sample['train']][['DriverId','dtYear']]

# cruza dos dados de piloto e ano, com as outras colunas e linhas da tabela completa
df_sample_train = df_sample_train.merge(df_train, on=['DriverId','dtYear'])
df_sample_train

# seleciona apenas as linhas marcadas como test e as colunas de piloto e ano
df_sample_test = df_sample[df_sample['test']][['DriverId','dtYear']]

# cruza dos dados de piloto e ano, com as outras colunas e linhas da tabela completa
df_sample_test = df_sample_test.merge(df_train, on=['DriverId','dtYear'])
df_sample_test

# %%

print("Base para treino/test completa:", df_train.shape[0])

print("Base treino:", df_sample_train.shape[0]) 
print("Base de teste:", df_sample_test.shape[0])

print("Base treino + test:", df_sample_train.shape[0] + df_sample_test.shape[0])

# %%

target = 'flChamp'
y_train = df_sample_train[target]

# %%

to_remove = ['DriverId', 'dtRef', target]

features = df_sample_train.columns.tolist()

for i in to_remove:
    features.remove(i)

X_train = df_sample_train[features]


# %%

nas = X_train.isna().sum()
nas[nas>0]

# %%

features_imput_99 = [
    'avgPositionSprint',
    'avgGridPositionSprint',
    'medianPositionSprint',
    'medianGridPositionSprint',
    'avgPositionSprint1Year',
    'avgGridPositionSprint1Year',
    'medianPositionSprint1Year',
    'medianGridPositionSprint1Year',
    'avgPositionSprintCurrentTemp',
    'avgGridPositionSprintCurrentTemp',
    'medianPositionSprintCurrentTemp',
    'medianGridPositionSprintCurrentTemp',
]

imput_99 = imputation.ArbitraryNumberImputer(arbitrary_number=99,
                                             variables=features_imput_99)

features_imput_0 = [
    'avgPositionSprintGain',
    'medianPositionSprintGain',
    'avgPositionSprintGain1Year',
    'medianPositionSprintGain1Year',
    'avgPositionSprintGainCurrentTemp',
    'medianPositionSprintGainCurrentTemp',
]

imput_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                             variables=features_imput_0)


# %%

clf = ensemble.RandomForestClassifier(random_state=42, min_samples_leaf=20)

model = pipeline.Pipeline(
    steps=[("imput_99", imput_99),
           ("imput_0", imput_0),
           ("random_forest", clf)]
)

# %%

with mlflow.start_run():
    
    mlflow.sklearn.autolog()

    model.fit(X_train, y_train)

    y_pred_train = model.predict(X_train)
    y_prob_train = model.predict_proba(X_train)

    acc_train = metrics.accuracy_score(y_train, y_pred_train)
    auc_train = metrics.roc_auc_score(y_train, y_prob_train[:,1])

    y_test = df_sample_test[target]
    X_test = df_sample_test[features]

    y_pred_test = model.predict(X_test)
    y_prob_test = model.predict_proba(X_test)

    acc_test = metrics.accuracy_score(y_test, y_pred_test)
    auc_test = metrics.roc_auc_score(y_test, y_prob_test[:,1])

    y_oot = df_oot[target]
    X_oot = df_oot[features]

    y_pred_oot = model.predict(X_oot)
    y_prob_oot = model.predict_proba(X_oot)

    acc_oot = metrics.accuracy_score(y_oot, y_pred_oot)
    auc_oot = metrics.roc_auc_score(y_oot, y_prob_oot[:,1])
    
    mlflow.log_metrics({
        "auc_train":auc_train,
        "auc_test":auc_test,
        "acc_oot":acc_oot,
    })
    
    
    columns = model[:-1].transform(X_train.head(1)).columns.tolist()
    feature_importance = pd.Series( model[-1].feature_importances_, index=columns)
    feature_importance = feature_importance.sort_values(ascending=False)
    feature_importance = feature_importance[feature_importance > 0]
    feature_importance

# %%

df_oot['predict'] = y_prob_oot[:,1]

# %%

columns_resume = [
    'tempRoundNumber',
    'DriverId',
    'dtRef',
    'dtYear',
    'predict',
    'qtdeWins1Year',
    'qtdePoles1Year',
    'medianPositionRaceCurrentTemp',
    'medianPosition1Year',
    'medianPositionRace1Year',
]

max_df = df_oot[df_oot['DriverId']=='max_verstappen'][columns_resume]
max_df


# %%

top_drivers = (df_oot[df_oot['tempRoundNumber']==df_oot['tempRoundNumber'].max()]
                    .sort_values(by='predict', ascending=False)
                    .head(5)['DriverId']
                    .unique()
                    .tolist())

df_top_drivers = df_oot[df_oot['DriverId'].isin(top_drivers)].sort_values('dtRef')
df_top_drivers
# %%
sns.lineplot(df_top_drivers, x='tempRoundNumber', y='predict', hue='DriverId')

# %%

spark_ops.create_view_from_path("../data/silver/fs_drivers", spark)

df_2025 = spark.table("fs_drivers").filter("dtRef > '2025-01-01'").toPandas()
df_2025['tempRoundNumber'] = df_2025['tempRoundNumber'].astype(int)

X_2025 = df_2025[features]

df_2025['predict'] = model.predict_proba(X_2025)[:,1]

df_2025 = df_2025[columns_resume]

top_05 = (df_2025[df_2025['tempRoundNumber']==df_2025['tempRoundNumber'].max()]
          .sort_values('predict', ascending=False)
          .head(5)
          ['DriverId']
          .tolist())

df_top_05 = df_2025[df_2025['DriverId'].isin(top_05)].sort_values('tempRoundNumber', ascending=False)


plt.figure(figsize=(9,8), dpi=400)
sns.lineplot(df_top_05, x='tempRoundNumber', y='predict', hue='DriverId', sort=True)

# %%

df_hist = pd.concat([df_top_drivers[columns_resume], df_top_05])
df_hist = df_hist.sort_values(by='dtRef')
# %%

plt.figure(figsize=(9,8), dpi=400)
sns.lineplot(df_hist, x='dtRef', y='predict', hue='DriverId', sort=True)
# %%
