'''
Vamos verificar o conteudo da base empilhada de ciclo de vida de cliente a partir do novo
banco analítico criado para esse fim e populado no arquivo anterior
'''

# %%

import pandas as pd
import sqlalchemy as sql
import matplotlib.pyplot as plt

# %%

engine = 'sqlite:///' + '../../data/analytics/database.db'
conn_analytical = sql.create_engine(engine)

query = 'SELECT * FROM life_cycle'

df = pd.read_sql(sql=query , con=engine)
df['dtRef'] = pd.to_datetime(df['dtRef'])

#df.dtypes
#df.shape

#%%

# Plotar grafico de barras empilhadas

colunas_plot = [
    '01-CURIOSO' ,
    '02-FIEL' ,	
    '02-RECONQUISTADO' ,
    '02-RENASCIDO',
    '03-TURISTA' ,	
    '04-DESENCANTADO'
]

fig , ax = plt.subplots()

# Criar tabela resumida
df_plot =(
 df.groupby(by=['dtRef', 'descLifeCycle'])['IdCliente']
   .size()
   .unstack(fill_value=0)
   .reindex(columns=colunas_plot , fill_value=0)
   #.reset_index()
)

df_plot.index = df_plot.index.strftime('%Y-%m')

ax = df_plot.plot(kind='bar' , stacked=True)
ax.tick_params(axis='x' , labelsize=8)
plt.tight_layout()
plt.show()

df_plot_100 = df_plot.div(df_plot.sum(axis=1) , axis=0)

ax = df_plot_100.plot(kind='bar' , stacked=True)
ax.tick_params(axis='x' , labelsize=8)
plt.tight_layout()
plt.show()
# %%
