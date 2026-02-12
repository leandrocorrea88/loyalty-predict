'''
Rodar as FS para uma lista de datas para todas as nossas Feature Stores. A ideia é transformar
esse arquivo em um EXECUTOR DE QUERIES, que roda e salva no banco de acordo com a query selecionada
'''

# %%

import pandas as pd
import sqlalchemy as sql
from tqdm import tqdm
import argparse # contolar os inputs do script

#%%

STANDARD_DATE_START = '2025-03-01'

QUERY_MAP = {

    "seg_clientes" : {
        "label" : "Segmentação de Clientes" ,
        "desc" : "Cria as métricas e segmentação de clientes" ,
        "query" : "05.Segmentacao_clientes_param" ,
        "table" : "life_cycle" ,
        "db_origin" : "loyalty-system" ,
        "db_target" : "analytics"
    } ,
    
    "fs_transacional" : {
        "label" : "FS Usuario Transacional" ,
        "desc" : "Cria features de transações por usuário" ,
        "query" : "07.FS_Usuario_Transacional" ,
        "table" : "fs_transacional" ,
        "db_origin" : "loyalty-system" ,
        "db_target" : "analytics"
    } ,

    "fs_educational" :{
        "label" : "FS Usuario Plataforma de Cursos" ,
        "desc" : "Cria features de transações nas plataformas de cursos" ,
        "query" : "08.FS_Usuario_Educational" ,
        "table" : "fs_educational" ,
        "db_origin" : "education-platform" ,
        "db_target" : "analytics"
    } , 

    "fs_lifecycle" : {
        "label" : "FS Usuario Ciclo de Vida" ,
        "desc" : "Cria features de ciclo de vida de usuário" ,
        "query" : "09.FS_Usuario_CicloDeVida" ,
        "table" : "fs_life_cycle" ,
        "db_origin" : "analytics" ,
        "db_target" : "analytics"
    }

}

# %%

def import_query(path):
    """Retorna o conteudo de uma query especifica"""
    # Usar encoding para tratar acentos e caracteres especiais
    with open(path, encoding='utf-8') as open_sql:
        query = open_sql.read()
    
    return query


def date_range(start , stop):
    """Listar datas entre um periodo inicial e final"""
    lista_datas = pd.date_range(start=start ,end=stop).to_list()
    # Converter timestamp para datas
    lista_datas = list(map(lambda lst : lst.strftime("%Y-%m-%d") , lista_datas))

    return lista_datas

# Mapear as queries com suas tabelas
def map_table(token:str):
    """Captura o indice do usuario e mapeia as variaveis"""
    
    if token not in QUERY_MAP:
        raise ValueError(f"Opção inválida: {token}. Use --list para ver as opções.")
    
    data = QUERY_MAP[token]
    
    return data['query'] , data['table'] , data['db_origin'] , data['db_target']

def exec_query(id_query , dt_start , dt_stop):
    """Função que dispara todo o ciclo de execução de uma query para uma faixa de datas"""
    
    map_query , map_tabela , db_origin , db_target = map_table(id_query)

    conn_leitura = sql.create_engine(f"sqlite:///../../data/{db_origin}/database.db")
    conn_gravacao = sql.create_engine(f"sqlite:///../../data/{db_target}/database.db")
    
    lst_datas = date_range(dt_start , dt_stop)
    query_exec = import_query(f"{map_query}.sql")

    # Testar a existencia da tabela (evita quebrar na primeira execução)
    inspector = sql.inspect(conn_gravacao)

    for i in tqdm(lst_datas):
        
        # Se a tabela existir, executa o DELETE, senão passa reto pra criar
        if inspector.has_table(map_tabela):
            with conn_gravacao.connect() as bd_analitco:
                # A marcação tem um offset de -1 DIA então aplicamos para sincronizar a DELEÇÂO
                query_delete = f"DELETE FROM {map_tabela} WHERE dtRef=DATE('{i}' , '-1 DAY')"
                bd_analitco.execute(sql.text(query_delete))
                bd_analitco.commit()

        query_exec_i = query_exec.format(_date=i)
        df = pd.read_sql_query(sql= query_exec_i , con=conn_leitura)

        # if i in lst_datas[:3]:
        #     print("\n---DEBUG---")
        #     print("data i : " , i)
        #     print("linhas : " , len(df))
        #     print("colunas : " , df.columns.tolist())
        #     print("sql (primeiros 500 chars) : " , query_exec_i[:500])

        df.to_sql(name=map_tabela , con=conn_gravacao , 
                  index=False , if_exists='append', 
                  # Inserir registro em lotes de 500 para performance 
                  method='multi' , chunksize=500)

    print(f"Total de {len(lst_datas)} lotes de registros adicionados!")

def print_query_options():
    """Função que vai alimentar o --list das opções de agumento"""
    print("\nLista de argumentos usados no script\n")

    print(f"Data incial padrão (dt_start) : {STANDARD_DATE_START}")
    STANDARD_DATE_STOP = pd.Timestamp.now().strftime('%Y-%m-%d')
    print(f"Data final padrão (dt_stop) : {STANDARD_DATE_STOP}")

    print("\nOpções disponíveis de queries:\n")
    for chave, item in QUERY_MAP.items():
        print(f"Token (query): {chave}")
        print(f"   Nome : {item['label']}")
        print(f"   Descrição : {item['desc']}")
        print(f"   Arquivo usado : {item['query']}.sql")
        print(f"   DB Origem : {item['db_origin']}")
        print(f"   DB Destino : {item['db_target']}")
        # Inserir uma "quebra de registro"
        print("-" * 50)

# Usamos o main() para controlar a execução automática a partir da chamada desse arquivo a partir
# do terminal
def main():

    STANDARD_DATE_STOP = pd.Timestamp.now().strftime('%Y-%m-%d')

    # Criar um coletor de argumentos para 
    parser = argparse.ArgumentParser(description="Executor de queries das Feature Stores")

    parser.add_argument('--query' , 
                        choices=list(QUERY_MAP.keys()) ,
                        help="Token a executar")
    
    parser.add_argument('--dt_start' , type=str , default=STANDARD_DATE_START)
    parser.add_argument('--dt_stop' , type=str , default=STANDARD_DATE_STOP)

    ## Adicionar o --list para visualizar o mapa atual, armazenando TRUE quando for passado pelo usuario
    parser.add_argument('--list' , action="store_true" , help="Lista o mapa de queries")

    # Aglutinar os argumentos
    args = parser.parse_args()

    # Se o usuario pediu a lista
    if args.list:
        print_query_options()
        return
    
    # Se não passou query e não pediu lista
    if not args.query:
        parser.error("Você precisa informar uma --query ou usar --list para ver opções")

    # Validar as datas, caso venha errada
    try:
        pd.to_datetime(args.dt_start)
        pd.to_datetime(args.dt_stop)
    except Exception:
            parser.error("Datas inválidas. Use YYYY-MM-DD. Ex: 2025-09-01")

    # Chamar a função passando
    exec_query(args.query , args.dt_start , args.dt_stop)

# Caso seja chamado pelo terminal o __name__ será executado, caso seja importado ele irá ignorar
# a execução de main()
if __name__ == "__main__":
    main()