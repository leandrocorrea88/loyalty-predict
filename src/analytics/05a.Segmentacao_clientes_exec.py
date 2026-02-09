'''
Rodar o loop nas datas de referencia para tirar as fotos da base de clientes no final de cada mês.

Para salvar os resultados, vamos criar um novo banco de dados, com contexto ANALITICO para garantir
alguins pontos, como:

    - Separação do banco de aplicação (produção)
    - Redução das chances de escrita errada sobre o banco oficial
    - Autonomia para modificar

O banco será criado na pasta data/analytics/ com o nome de "analytics". Ao executar a query de gravação
o arquivo é criado automaticamente
'''

# %%

import pandas as pd
import sqlalchemy as sql

# %%

# Como vamos executar em loop vamos criar uma função para executar uma query especifica usando
# conexão
def import_query(path):
    # Retornar o conteudo de um arquivo SQL (Texto)
    with open(path) as open_sql:
        query = open_sql.read()
    return query

# %%

# Invocar a função
query = import_query('05.Segmentacao_clientes_param.sql')
print(query)

# Invocando a função usando o PLACEHOLDER
print(query.format(_date='2025-08-31'))

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

# Gerar uma lista de datas entre a primeira e a ultima com a frequencia
# MONTH START (Início do Mês). IMPORTANTE: Como a frequencia definida foi
# MS, a primeira data está FORA desse range (27-01), então não aparece na
# listagem final, mas é usada para a composição da lista
lst_datas_corte = pd.date_range(start=df_datas['dtPrimeiraTransacao'][0] ,
                                end=df_datas['dtUltimaTransacao'][0] ,
                                freq='MS').to_list()

# Adicionamos um mes subsequente para o caso de a ultima data ser após o ultimo
# elemento da lista
if lst_datas_corte[-1] < df_datas['dtUltimaTransacao'][0]:
    nova_data = lst_datas_corte[-1] + pd.DateOffset(months=1)
    lst_datas_corte.append(nova_data)

# Agora temos uma lista de TimeStamps com o início de cada mês

# %%

## (4) Consumindo a lista de datas para realizar as consultas
query_life_cycle = import_query('05.Segmentacao_clientes_param.sql')

# Montar o loop
for i in lst_datas_corte:

    # Converter TimeStamp para um formato que a query leia
    data_corte = i.strftime("%Y-%m-%d")
    print(f"Gerando dia - {data_corte}")

    try:    # Limpar os dados que possam existir com essa mesma data na base ANALITCA
        with conn_gravacao.connect() as bd_analitco:
            # A marcação tem um offset de -1 DIA então aplicamos para sincronizar a DELEÇÂO
            query_delete = f"DELETE FROM life_cycle WHERE dtRef=DATE('{data_corte}' , '-1 DAY')"
            bd_analitco.execute(sql.text(query_delete))
            bd_analitco.commit()
    except Exception as err:
        print(err)

    # Com os dados excluidos podemos repopular a base com o corte
    query_life_cycle_i = query_life_cycle.format(_date=data_corte)
    df = pd.read_sql_query(sql= query_life_cycle_i , con=conn_leitura)

    # Salvar no banco analítico
    df.to_sql(name='life_cycle' , con=conn_gravacao , index=False , if_exists='append')
# %%
