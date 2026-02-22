'''
Nesse arquivo vamos consumir as informações dos modelos disponiveis no nosso servidor
MLFlow, carregando desse ambiente em vez de acessar usando um PICKLE
'''

#%%

import pandas as pd
import sqlalchemy as sql
import mlflow
import json

#%%

# Configurando o ambinente
mlflow.set_tracking_uri(uri='http://127.0.0.1:5000')
mlflow.set_experiment(experiment_id=1)

#%%

# Importando os dados
conn = sql.create_engine('sqlite:///../../data/analytics/database.db')
df = pd.read_sql('SELECT * FROM abt_flFiel' , conn)

#%%

# Cpturar as versões dos modelos REGISTRADOS
versions = mlflow.search_model_versions(filter_string="name='model_fiel'")
# Identificar a ultima versão
latest_version = max([int(i.version) for i in versions])
# Carregar o modelo registrado mais atual
model_uri=f"models:/model_fiel/{latest_version}"
model = mlflow.sklearn.load_model(model_uri=model_uri)

#%%

# obter o maior OBJETO versions usando o max de version como chave
lastest_obj = max(versions , key=lambda v:int(v.version))

# Capturar o ID da RUN (que é onde os artefatos estão armazenados)
run = lastest_obj.run_id

# Instanciar a API
client = mlflow.tracking.MlflowClient()

# Baixar localmente o arquivo (path é o nome. Para listar podemos usar o LIST_ARTIFACTS)
local_path = client.download_artifacts(run_id=run,path="raw_features.json")

# Acessar a pasta de artifacts (onde o raw_features.json está criado)
with open(file=local_path, mode='r') as file:
    # Capturar as features
    features = json.load(file)['features']
# %%

# Usar o modelo para prever
predict = model.predict_proba(df[features])[:,1]
df['predict'] = predict