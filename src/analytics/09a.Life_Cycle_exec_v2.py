'''
Rodar o loop para criar a posição  de Life Cycle em cada uma das datas desde o primeiro até o último
registro constante na base
'''

# %%

import pandas as pd
import sqlalchemy as sql
from tqdm import tqdm

# %%

# Como vamos executar em loop vamos criar uma função para executar uma query especifica usando
# conexão
def import_query(path):
    # Retornar o conteudo de um arquivo SQL (Texto)
    with open(path) as open_sql:
        query = open_sql.read()
    return query

# %%
####################
# Executando o Loop
####################

#%%

## (1) Criar a conexão com o banco 1 (Engine + Path)
string_leitura = 'sqlite:///' + '../../data/loyalty-system/database.db'
conn_leitura = sql.create_engine(string_leitura)

## (2) Criar a conexão com o banco 2 (Engine + Path)
string_gravacao = 'sqlite:///' + '../../data/analytics/database.db'
conn_gravacao = sql.create_engine(string_gravacao)

# %%

## (3) Montando a lista de datas
query_datas = import_query('03b.Datas_loop.sql')
df_datas = pd.read_sql_query(sql=query_datas , con=conn_leitura)

# Converter o campo para DATA
df_datas = df_datas.apply(lambda row : pd.to_datetime(row) )

# Agora não precisamos mais pular no inicio de cada mês
lst_datas_corte = pd.date_range(start=df_datas['dtPrimeiraTransacao'][0] ,
                                end=df_datas['dtUltimaTransacao'][0]).to_list()

# Agora temos uma lista de TimeStamps

# %%

## (4) Consumindo a lista de datas para realizar as consultas
query_life_cycle = import_query('05.Segmentacao_clientes_param.sql')

# Montar o loop assistido por TQDM para acompanhar o progresso
for i in tqdm(lst_datas_corte):

    # Converter TimeStamp para um formato que a query leia
    data_corte = i.strftime("%Y-%m-%d")
    # Comentar a linha porque o TQDM zoa com prints
    #print(f"Gerando dia - {data_corte}")

    # Limpar os dados que possam existir com essa mesma data na base ANALITCA
    with conn_gravacao.connect() as bd_analitco:
        # A marcação tem um offset de -1 DIA então aplicamos para sincronizar a DELEÇÂO
        query_delete = f"DELETE FROM life_cycle WHERE dtRef=DATE('{data_corte}' , '-1 DAY')"
        bd_analitco.execute(sql.text(query_delete))
        bd_analitco.commit()

    # Com os dados excluidos podemos repopular a base com o corte
    query_life_cycle_i = query_life_cycle.format(_date=data_corte)
    df = pd.read_sql_query(sql= query_life_cycle_i , con=conn_leitura)

    # Salvar no banco analítico
    df.to_sql(name='life_cycle' , con=conn_gravacao , index=False , if_exists='append')

print(f"Total de {len(lst_datas_corte)} registros adicionados!")
# %%
