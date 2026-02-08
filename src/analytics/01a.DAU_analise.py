'''
Análise e visualização dos dados de DAU obtidos na consulta à base de dados de loyalty-system
'''

# %%

# Referencias
import pandas as pd
import sqlite3 as sq3
import matplotlib.pyplot as plt

# %%

# Configurar a conexão com a base de dados
conn = sq3.connect('../../data/loyalty-system/database.db')

# Ler a query que está escrita e testada no arquivo de texto
with open("01.DAU_query.sql" , "r") as queryfile:
    query = queryfile.read()

# %%

# Carregar o resultados da query em um Pandas DF
df = pd.read_sql_query(query , conn)
df['DtDia'] = pd.to_datetime(df['DtDia'])
df.sort_values(by='DtDia', ascending=True , inplace=True)

# %%

# controlar o eixo X para exibir as datas em meses
import matplotlib.dates as mdates

# Visualizar a métrica plotada temporalmente
fig , ax = plt.subplots()

ax.plot( df['DtDia'], df['DAU'], linewidth=0.7 )
ax.set_title("DAU por dia")
ax.set_ylabel("DAU")
ax.set_xlabel("Data" )

# Controle do eixo X - intervalos variaveis de meses em formato amigavel
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt='%b-%Y'))
ax.tick_params(axis='both' , labelsize=7)
ax.grid(visible=False)

# Rotacionar os rotulos do eixo X
plt.setp(ax.get_xticklabels(), rotation=90, ha='center')
plt.tight_layout()
plt.show()

'''
Olhando a série histórica vemos uma variação grande do DAU, justamente pela natureza da métrica de 
ser bastante sensível a variações. 

(a) Os pontos mais baixos são os registros de usuários "farmando" pontos em fds, quando o bot se 
mantem online para gararntir o processamento do !presente para os que fazem.

(b) O ano de 2024 teve uma tendencia de queda, com o ano de 2025 andando de lado

(c) Os picos são alcançados quando algum curso na plataforma é liberado, aumentando o engajamento,
mas após o curso os valores tendem a voltar para o patamar anterior ou se deslocam para uma nova
média superior à anterior

Vamos tentar olhar os valore de medias mensais para ver um comportamento mais alto nivel

'''

# %%

# Agregando médias mensais

# Aqui podemos criar uma nova coluna com os meses e agrupar os dados para devolver os valores para
# o DF original
df['MesAno'] = df['DtDia'].dt.strftime('%Y-%m')
df.groupby('MesAno')['DAU'].mean()

# Porém para não perdermos podemos usar uma estrutura mais enxuta que já devolve os valores para
# O DF, usando a seguinte estrutura que já cria um agrupamento por mes
df['DtDia'].dt.to_period('M')

# Mas usando o transform não vamos precisar criar essa nova coluna no DF
df = df.drop('MesAno', axis=1)

# Agora usando as funções de agregação pra fazer isso em uma nova coluna
df['DAU_Mensal'] = (
    df
    .groupby(df['DtDia'].dt.to_period('M'))['DAU']
    .transform('mean')
)

#%%

# e podemos devolver a serie ao grafico para enxergar os padrões

# Visualizar a métrica plotada temporalmente
fig , ax = plt.subplots()

# Series
ax.plot( df['DtDia'], df['DAU'], linewidth=0.7 , alpha=0.4 , label='DAU diário' )
ax.plot( df['DtDia'] , df['DAU_Mensal'] , linewidth=1 , color='red' , label='DAU mensal')

ax.set_title("DAU dário vs DAU mensal" , loc='left')
ax.set_ylabel("DAU")
ax.set_xlabel("Data" )

# Controle do eixo X - intervalos variaveis de meses em formato amigavel
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt='%b-%Y'))
ax.tick_params(axis='both' , labelsize=7)
ax.grid(visible=False)

# Rotacionar os rotulos do eixo X
plt.setp(ax.get_xticklabels(), rotation=90, ha='center')
plt.tight_layout()
plt.show()


# %%

# Plotando histograma dos meses - EDUCACIONAL, não analítico

'''
Primeiro vamos plotar o geral. Lembrando que essa abordagem para series temporais deve ser feita
com muito cuidado, pois estamos lidando com um fenomeno que pode variar ao longo do tempo, então
a lógica de outlier deve ser aplicada em porções menores (ex.: Mes)
'''

fig , ax = plt.subplots()

ax.hist(x=df['DAU'] , bins=30) # o BINS tambem aceita métodos como 'sturges', 'sqrt' ou outros
ax.set_title("Histograma Geral de DAU")
ax.set_xlabel("Faixas de DAU")
ax.set_ylabel("Frequencia")

plt.tight_layout()
plt.show()

# %%

# Plotando uma serie de meses vs ano em grade
import numpy as np

# Vamos criar uma copia do DF para garantir que não vamos mexer na base de dados que veio da QUERY
df_histo = df.copy()

# Removendo valores faltantes e criando as novas colunas
df_histo = df_histo.dropna(subset=['DtDia', 'DAU'])
df_histo = df_histo.sort_values(by='DtDia')
df_histo['Ano'] = df_histo['DtDia'].dt.year
df_histo['Mes'] = df_histo['DtDia'].dt.month

# Criar os 12 subplots, com overlay de ano
anos = sorted(df_histo['Ano'].unique())
meses = range(1 , 13)

# Criar os bins globais usando sturges
bins = np.histogram_bin_edges(df['DAU'] , bins='sturges')

# Criar os plots (Grade de 3x4 compartilhando os mesmos eixos X e Y)
fig , axes = plt.subplots(nrows=3 , ncols=4, figsize=(16,10) ,
                          sharex=True , sharey=True )

# o objeto AXIS criou um array de 2 dimensões (3x4) e para itera-lo podemos aninhar for usando as
# coordenadas de X e Y, porém pode ser meio problemático de controlar. Então, vamos tranformar
# esse array em UNIDIMENSIONAL, assim podendo iterar em somente uma dimensão
axes = axes.ravel()

# Agora iteramos em cada subplot da grade. Usamos o ENUMERATE em Mes para criar um iterável que
# cria um INDICE (do range) e um sequencial (1-12), desempacotando nas variaveis i (indice) e
# mes (valor do mes 1-12)
for i , mes in enumerate(meses):
    
    # Selecionar o elemento em subplots e extrair um set do DF correspondente em um novo DF temp
    ax = axes[i]
    df_temp = df_histo[df_histo['Mes']==mes]

    # Crias as séries por ano para sobrepor em cada histograma
    for ano in anos:
        # Pegar apenas um ano da iteração, na posição DAU
        x = df_temp.loc[df_temp['Ano'] == ano ,'DAU' ].values
        if len(x) == 0: # se um valor não existir para um ano, passar para a proxima iteração
            continue
        # Plotar o histograma de densidades, porque queremos ver distribuição
        ax.hist(x , bins=bins , alpha=0.35 , density=True , label=str(ano))
    
    # Padronizar titulo do grafico preenchendo com ZERO e mantendo o padrão de 2 DIGITOS em um
    # DECIMAL (inteiro)
    ax.set_title(f"Mês {mes:02d}")
    ax.tick_params(axis='both' , labelsize=8)

# Criar uma legenda unica FORA DA GRADE
handles , labels = axes[0].get_legend_handles_labels()
fig.legend(handles=handles , labels=labels , loc='upper center' , 
           ncol=min(len(anos) , 6) , frameon=False)

fig.suptitle("Distribuição de DAY por mes (vs anos)" , y=1.02 , fontsize=14)
plt.tight_layout()
plt.show()

# %%
