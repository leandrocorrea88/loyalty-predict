import pandas as pd
import sqlalchemy as sql

conn = sql.create_engine('sqlite:///../../data/analytics/database.db')

#%%

# Import dos dados e modelo

df = pd.read_sql('SELECT * FROM abt_flFiel' , conn)
model = pd.read_pickle('13a.model_fiel.pkl')

# %%

predict = model['model'].predict_proba(df[model['raw_features']])[:, 1]
df['predict'] = predict
# %%
