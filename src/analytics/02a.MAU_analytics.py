'''
Nesse bloco vamos explorar um pouco mais dos dados, agora olhando par ao MAU calculado pela QUERY.
Vamos entender outros conceitos aqui usando esse mesmo indicador,  ainda que de maneira bem
educacional, visto que alguns métodos de análise possam não ser os mais recomendados para cada
tipo de dado analisadp
'''
#%%

# Referencias
import pandas as pd
import sqlalchemy as sql
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# %%

# Criar a ENGINE de Conexão com o banco 
# (Para efeito didático separei a STRING/PROTOCOLO da LOCALIZAÇÃO do arquivo
string_conexao = 'sqlite:///' + '../../data/loyalty-system/database.db'
engine = sql.create_engine(string_conexao)

# Conectar ao banco usando a instrução SQL da métrica
with open(file='02.MAU_query.sql' , mode='r') as file:
    query = file.read()

# Criar o DF
df = pd.read_sql_query(sql=query , con=engine)

df['DtRef'] = pd.to_datetime(df['DtRef'])

# %%

# Criar a coluna de média mensal
df['MAU Media Mes'] = (
    df
    .groupby(df['DtRef'].dt.to_period('M'))['MAU']
    .transform('mean')
)

# Plotar o grafico usando a suavização mensal
fig , ax = plt.subplots()

ax.plot(df['DtRef'] , df['MAU'] , linewidth=0.7 , alpha=0.4 , label='MAU diário')
ax.plot(df['DtRef'] , df['MAU Media Mes'] , linewidth=1 , label='MAU Media Mes')

ax.set_title('MAU Diário vs MAU Mensal')
ax.set_xlabel("Data")
ax.set_ylabel("MAU")

ax.xaxis.set_major_locator(locator=mdates.MonthLocator(interval=4))
ax.xaxis.set_major_formatter(formatter=mdates.DateFormatter('%b-%Y'))

plt.tight_layout()
plt.show()

# %%

