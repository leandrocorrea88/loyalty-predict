
#%%
import pandas as pd
import sqlalchemy as sql


conn = sql.create_engine('sqlite:///../../data/analytics/database.db')

#%%

# SAMPLE - Import dos dados

df = pd.read_sql('abt_flFiel' , conn)
df.head()
# %%

# SAMPLE - OOT

# IMPORTANTE : Precisamos garantir que os dados não enviesados não sejam usados 
# para TREINAR o modelo, mas para VALIDAR podemos usar bases enviesadas (no nosso
# caso, um viés é a autocorrelação)

df_oot = df[df['tpDado']=='OOT'].reset_index(drop=True)
df_oot
# %%

# SAMPLE - ABT
target = 'flFiel'
features = df.columns.to_list()[4:]

features.remove('avgFreqGrupo') # coluna não deveria estar aqui (EXPLORE MISSING P2)

df_abt = df[df['tpDado']=='ABT'].reset_index(drop=True)

X = df_abt[features]
y = df_abt[target]

from sklearn import model_selection

# Declarar explicitamente para garantir o Intellisense
X_train : pd.DataFrame
X_test : pd.DataFrame
y_train : pd.Series
y_test : pd.Series

X_train , X_test , y_train , y_test = model_selection.train_test_split(
    X, y , 
    test_size=0.2 , 
    random_state=42 , 
    stratify=y
)

print(f"Base Treino : {y_train.shape[0]} Unid | Tx. Target : {100 * y_train.mean():.2f}%")
print(f"Base Teste : {y_test.shape[0]} Unid | Tx. Target : {100 * y_test.mean():.2f}%")
# %%

# EXPLORE (somente Base de Treino) - MISSING

# Usar o isna() para atribuir 1 aos faltantes e mean() para o % de faltantes
# s_nas = X_train.isna().mean()
# Capturar cada variavel que tem Media > 0 (tem muito dado faltante)
# s_nas = s_nas[s_nas>0]

# Vamos montar um DF para ver o percentual de nulos em cada coluna e poder enxergar
# o que podemos fazer com cada uma delas (essa instrução replica o que foi feito acima
# mas armazenando os resultados em um DF)
dic_Features = (
    pd.DataFrame(data=X_train.isna().mean())              # Tirar a média de ocorrência do evento
      .reset_index()                                            
      .rename(columns={"index":"feature" ,                # Renomear colunas
                       0 : "pctNulos"})
      .assign(
          temNulo = lambda x : x['pctNulos'] > 0 ,        # Criar um bool de TemNulo
          action_fill = lambda x : x['temNulo'].map({     # Mapear ações a partir do bool
              True: 'pendente' , False : 'manter'
          }) ,
          notas = None ,                                  # Criar coluna de Notas
          etapa_pipe = None ,                             # Preparar dicionario para pipeline
          tipo_campo = None
        )
      .set_index(keys=['feature'])                        # Setar indice para facilitar gestão
)

# Fitrar as colunas com nulos
print(dic_Features[dic_Features['temNulo']].to_string())

# %%

# EXPLORE (somente Base de Treino) - MISSING P2

'''
Conclusões 

C1) Para todos os CURSOS podemos substituir os vazios por ZERO, uma vez que a tradução desse valor
é "o usuário não fez o curso" (índices 57 a 84)

C2) avgIntervaloDiasVida [43] , avgIntervaloDiasD28 [44] , qtdeDiasUltAtividade [85] são campos que
zerar vai dar ideia de alta frequencia. Jogar numero alto

C3) qtdeCursosCompletos [55] , qtdeCursosIncompletos [56] podemos imputar ZEROS

IMPORTANTE : As colunas de pctCiclo de Vida [89-95] não estão zeradas, porém, ao capturar um NOVO
usuário pode ser necesario criar uma regra de imputação

C4) descLifeCycleD28 [88] fica vazio quando o usuário nao tinha descrição 28 dias atras, ou seja, 
estamos falando de novos usuários. Não podemos iguala-los a 01-CURIOSO porque foge da regra definida
para o cluster, então podemos criar um novo valor 'MISSING'

C5) qtdeFrequencia [87] , ratioFreqGrupo [96] , ratioFreqUsuarioD7 [99], ratioFreqUsuarioD14 [100]
estão zerados nos mesmos 123 registros em usuários que não apresentaram frequencia durante o intervalo
do MAU. Podemos associar ZERO para esses valores

'''

# %%

# Features categoricas
feat_categoricas = ['descLifeCycleAtual', 'descLifeCycleD28']

# Features de cursos
feat_float_cursos = [
    'Carreira', 'ColetaDados2024', 'DataPlatform2025', 'DsDatabricks2024',
    'DsPontos2024', 'Estatistica2024', 'Estatistica2025', 'GitHub2024',
    'GitHub2025', 'Go2026', 'IaCanal2025', 'LagoMago2024',
    'LoyaltyPredict2025', 'MachineLearning2025', 'MatchmakingTramparDeCasa2024', 'Ml2024',
    'MlFlow2025', 'Nekt2025', 'Pandas2024', 'Pandas2025',
    'Python2024', 'Python2025', 'SpeedF1', 'SQL2020',
    'SQL2025', 'Streamlit2025', 'TramparLakehouse2024', 'TSEAnalytics2024',
 ]

# Features numericas inteiras para zero
feat_int_tozero = [
    'qtdeFrequencia' ,
    'qtdeAtivacaoVida', 'qtdeAtivacaoD7', 'qtdeAtivacaoD14', 'qtdeAtivacaoD28',
    'qtdeAtivacaoD56', 'qtdeTransacaoVida', 'qtdeTransacaoD7', 'qtdeTransacaoD14',
    'qtdeTransacaoD28', 'qtdeTransacaoD56', 'qtdePtosSaldoVida', 'qtdePtosSaldoD7',
    'qtdePtosSaldoD14', 'qtdePtosSaldoD28', 'qtdePtosSaldoD56', 'qtdePtosPositVida',
    'qtdePtosPositD7', 'qtdePtosPositD28', 'qtdePtosPositD14', 'qtdePtosPositD56',
    'qtdePtosNegatVida', 'qtdePtosNegatD7', 'qtdePtosNegatD28', 'qtdePtosNegatD14',
    'qtdePtosNegatD56', 'qtdeTransacoesManha', 'qtdeTransacoesTarde', 'qtdeTransacoesNoite'
]

# Features numericas float para zero
feat_float_tozero = [
    'qtdeTransacaoDiaVida', 'qtdeTransacaoDiaD7','qtdeTransacaoDiaD14', 
    'qtdeTransacaoDiaD28', 'qtdeTransacaoDiaD56' ,
    'pctAtivacaoMAU', 'pct01_CURIOSO', 'pct02_FIEL', 'pct02_RECONQUISTADO', 
    'pct02_RENASCIDO', 'pct03_TURISTA', 'pct04_DESENCANTADO', 'pct05_ZUMBI' ,
    'ratioFreqGrupo' , 'ratioFreqUsuarioD7' , 'ratioFreqUsuarioD14' ,
    'pctTransacoesManha', 'pctTransacoesTarde', 'pctTransacoesNoite' ,
    'qtdeHorasVida', 'qtdeHorasD7', 'qtdeHorasD14', 'qtdeHorasD28', 'qtdeHorasD56' , 
    'qtdeRPG', 'qtdeChurnModel', 'qtdeChatMessage','qtdeAirflowLover', 
    'qtdeRLover', 'qtdeResgatarPonei', 'qtdeListaDePresença', 
    'qtdePresençaStreak', 'qtdeTrocaDePontos', 'qtdeReembolsoDePontos' ,
    'qtdeCursosCompletos', 'qtdeCursosIncompletos'
]

# Features numericas para numero alto (ex.: 1.000)
feat_float_to1000 = [
    'avgIntervaloDiasVida', 'avgIntervaloDiasD28' , 'avgFreqD7', 'avgFreqD14' ,
    'qtdeDiasUltAtividade'
]

# Features que provocariam a exclusão da amostra
feat_int_exclude = [ 'idadeDias' ]

# Composição do dicionario
dic_dicionario = {
    "feat_categoricas" : {
        "features" : feat_categoricas ,
        "action_fill" : "Fill MISSING" ,
        "notas" : "Indica que o usuário não existia no recorte",
        "etapa_pipe" : "fill_missing" ,
        "tipo_campo" : "str"
    } ,
    "feat_cursos" : {
        "features" : feat_float_cursos ,
        "action_fill" : "ZERAR" ,
        "notas" : "0 indica que o curso não foi feito",
        "etapa_pipe" : "fill_cursos" ,
        "tipo_campo" : "float"
    } ,
    "feat_int_tozero" : {
        "features" : feat_int_tozero ,
        "action_fill" : "ZERAR" ,
        "notas" : "Features Maior-Melhor. 0 indica pior cenário",
        "etapa_pipe" : "fill_tozero" ,
        "tipo_campo" : "Int64"
    } ,
    "feat_float_tozero" : {
        "features" : feat_float_tozero ,
        "action_fill" : "ZERAR" ,
        "notas" : "Features Maior-Melhor. 0 indica pior cenário",
        "etapa_pipe" : "fill_tozero" ,
        "tipo_campo" : "float"
    } ,
    "feat_float_to1000" : {
        "features" : feat_float_to1000 ,
        "action_fill" : "Imputar 1000" ,
        "notas" : "Features Menor-Melhor. 1000 indica pior cenário",
        "etapa_pipe" : "fill_to1000" ,
        "tipo_campo" : "float"
    } ,
    "feat_int_exclude" : {
        "features" : feat_int_exclude ,
        "action_fill" : "Excluir amostra" ,
        "notas" : "Erro de calculo de idade indica erro critico da amostra",
        "etapa_pipe" : "remove_sample" ,
        "tipo_campo" : "Int64"
    }
}


# %%

# Atualização do dicionario
for chave, item in dic_dicionario.items():
    dic_Features.loc[item['features'] , 'action_fill'] = item['action_fill']
    dic_Features.loc[item['features'] , 'notas'] = item['notas']
    dic_Features.loc[item['features'] , 'etapa_pipe'] = item['etapa_pipe']
    dic_Features.loc[item['features'] , 'tipo_campo'] = item['tipo_campo']

# Atualizacao das listas
feat_numericas = [feat for feat in features if feat not in feat_categoricas]

# Ver pendencias de configuração de features
# condicao_print = (dic_Features['temNulo']) & (dic_Features['action_fill']=='pendente')
condicao_print = dic_Features['etapa_pipe'].isna()
print(dic_Features[condicao_print].index)

#%%

# EXPLORE (somente Base de Treino) - ANÁLISE BIVARIADA P1:Numerica 
# (Covariaveis vs Variavel)

# Separar as variaveis categoricas da base das numericas

# MODO 1 - Filtrando colunas por TIPO
# cat_features = X_train.dtypes[X_train.dtypes == 'object'].index.tolist()
# Esse método acima poderia converter todos os campos rapidamente, porém o python ja tipou
# os campos, então vamos ter que ir manualmente

# MODO 2 - Filtrando por regras especificas
# Capturar as COLUNAS que começam com "qtde", "avg", "ratio" ou tem o nome especifico
# num_features = [
#     col
#     for col
#     in X_train.columns.to_list()
#     if col.startswith(('qtde', 'avg', 'ratio', 'pct')) or col == 'idadeDias'
# ]

# cat_features = [
#     col
#     for col
#     in X_train.columns.to_list()
#     if col not in num_features
# ]

# MODO 3 - Usando LIST COMPREHENSION E SET
'''
Como ja criamos as lists dos campos categoricos (feat_categoricas) vamos consumi-las aqui

cat_features = [ col for col in X_train.columns.to_list() if col.startswith('desc') ]

Atenção que o SET remove duplicadas e PODE ALTERAR A ORDEM
num_features = list(set(features) - set(cat_features))

Aqui mantem a ordem
num_features = [ col for col in X_train.columns.to_list() if col not in cat_features ]
'''
# Agora criamos nnovos df

# Explorar features que possam ajudar mais a explicar a target
df_train = X_train.copy()
df_train[target] = y_train.copy()

print(df_train.to_string())

df_train[target] = df_train[target].astype(int)

# Converter tipos do DF usando o DF de Dicionario. 
# Primeiro criar um dicionario para iterar sobre
mapa_tipos = dic_Features['tipo_campo'].to_dict()
for chave , valor in mapa_tipos.items():
    # Converter cada campo do DF usando os valores
    df_train[chave] = df_train[chave].astype(valor)

# Printar a média
print(df_train.groupby(target)[feat_numericas].mean().T.to_string())

# Printar mediana, pra ser menos sensível
bivar_num = df_train.groupby(target)[feat_numericas].median().T
print(bivar_num.to_string())

# Agora baseado nisso podemos ordenar nosso DF de bivariada pelas diferenças (0.001 pra evitar Inf)
bivar_num['ratio'] = (bivar_num[1] + 0.001) / (bivar_num[0] + 0.001)
# e temos uma ideia de quais variaveis podem nos ajudar a explicar mais, ou menos, o target
print(bivar_num.sort_values(by='ratio', ascending=False).to_string())

# Então para melhorar a performance de calculo podemos remover as variaveis que tem
# ratio = 1 porque o comportamento delas entre os grupos nao muda, ou seja, não vão
# fazer diferença no modelo (Aplicar no MODIFY)
regra_remoção = (bivar_num['ratio'] == 1) | (bivar_num['ratio'].isna())
to_remove = bivar_num[regra_remoção].index.tolist()


#%%

# EXPLORE (somente Base de Treino) - ANÁLISE BIVARIADA P2:Categorica 

# Para as CATEGORICAS precisamos INVERTER o groupby
for i in feat_categoricas:
    print(df_train.groupby(i)[target].mean().T)
    print("\n")

'''
                    descLifeCycleAtual      descLifeCycleD28
01-CURIOSO          0.058824                0.058394
02-FIEL             0.351852                0.322335
02-RECONQUISTADO    0.062500                0.200000
02-RENASCIDO        0.019608                0.097561
03-TURISTA          0.035200                0.112782
04-DESENCANTADO     0.015361                0.072848
05-ZUMBI            0.010526                0.031079

Ali vemos as probabilidades de alguem ser FIEL hoje baseado no seu grupo atual e 28dias
atrás, com clara vantagem para 02-FIEL e menor desempenho para ZUMBI

'''

# %%

# MODIFY (Modo rudimentar)

#%%

'''
Aqui vamos usar o dicionario que foi criado para gerencias os campos que farão parte de cada etapa
do pipeline, lembrando que temos as macroetapas:

1. Conversão dos campos
2. Remoção dos campos rejeitados pelo ratio da bivariada
3. Aplicação dos imputs
4. Aplicação do modelo
'''

# %%

# MODIFY - Converter campos

# Vamos usar o que ja testamos anteriormente para converter, mas agora aplicando em X_train
mapa_tipos = dic_Features['tipo_campo'].to_dict()
for chave , valor in mapa_tipos.items():
    # Converter cada campo do DF usando os valores
    X_train[chave] = X_train[chave].astype(valor)


#%%
# MODIFY - DROP (Tirar variaveis irrelevantes)

from feature_engine import selection

regra_remoção = (bivar_num['ratio'] == 1) | (bivar_num['ratio'].isna())
to_remove = bivar_num[regra_remoção].index.tolist()

drop_features = selection.DropFeatures(to_remove)
X_train_transform = drop_features.fit_transform( X_train )

#%%

# Retomar as etapas já mapeadas
dic_Features['etapa_pipe'].unique()
# %%

# MODIFY - IMPUTATION (Definir Regras)

from feature_engine import imputation

# Zerar os numericos aptos que estejam na base pós DROP
cols_fill_tozero = (
    dic_Features.loc[dic_Features['etapa_pipe'] == 'fill_tozero']
    .index
    .intersection(X_train_transform.columns)
    .to_list()
)
imput_fill_tozero = imputation.ArbitraryNumberImputer(arbitrary_number=0 ,
                                            variables=cols_fill_tozero)

# Aumentar os valores de features menor-melhor que estejam na base pós DROP
cols_fill_to1000 = (
    dic_Features.loc[dic_Features['etapa_pipe'] == 'fill_to1000']
    .index
    .intersection(X_train_transform.columns)
    .to_list()
)
imput_fill_to1000 = imputation.ArbitraryNumberImputer(arbitrary_number=1000,
                                                      variables=cols_fill_to1000)

# Colunas de cursos com 0 quando não fez que estejam na base pós DROP
cols_fill_cursos = (
    dic_Features.loc[dic_Features['etapa_pipe'] == 'fill_cursos']
    .index
    .intersection(X_train_transform.columns)
    .to_list()
)
imput_fill_cursos = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                      variables=cols_fill_cursos)

# Pessoas que não existiam 28 dias atrás
cols_fill_missing = (
    dic_Features[dic_Features['etapa_pipe'] == 'fill_missing']
    .index
    .intersection(X_train_transform.columns)
    .to_list()
)
imput_fill_missing = imputation.CategoricalImputer(fill_value='Não-usuário' ,
                                                   variables=cols_fill_missing)

#%%

from feature_engine import encoding

# MODIFY - ONE HOT ENNCODER
cols_one_hot = (
    dic_Features[dic_Features['tipo_campo'] == 'str']
    .index
    .intersection(X_train_transform.columns)
    .to_list()
)
imput_onehot = encoding.OneHotEncoder(variables=cols_one_hot)


# %%

# MODIFY - IMPUTATION (Aplicar regras)

# Sempre aplicando a ultima versão transformada
X_train_transform = imput_fill_tozero.fit_transform(X_train_transform)
X_train_transform = imput_fill_to1000.fit_transform(X_train_transform)
X_train_transform = imput_fill_cursos.fit_transform(X_train_transform)
X_train_transform = imput_fill_missing.fit_transform(X_train_transform)
X_train_transform = imput_onehot.fit_transform(X_train_transform)

X_train_transform.columns = X_train_transform.columns.map(str)


# %%
# Rodamos a descritiva novamente para ver os tratamentos
s_na = X_train_transform.isna().mean()
s_na[s_na>0].index.tolist()
# %%

# MODEL

from sklearn import tree
from sklearn import ensemble

# model = tree.DecisionTreeClassifier(random_state=42)
model = ensemble.RandomForestClassifier(n_estimators=150 ,
                                        min_samples_leaf=30,
                                        n_jobs=-1)
model.fit(X_train_transform , y_train)
# %%

# ASSESS

from sklearn import metrics
import scikitplot as skplot

y_pred_train = model.predict(X_train_transform)
y_proba_train = model.predict_proba(X_train_transform)

metrics.accuracy_score(y_train , y_pred_train)
metrics.roc_auc_score(y_train , y_score=y_proba_train[:,1])

# %%

# Validar os dados de TESTE
X_test_transform = drop_features.transform( X_test )
X_test_transform = imput_fill_tozero.transform(X_test_transform)
X_test_transform = imput_fill_to1000.transform(X_test_transform)
X_test_transform = imput_fill_cursos.transform(X_test_transform)
X_test_transform = imput_fill_missing.transform(X_test_transform)
X_test_transform = imput_onehot.transform(X_test_transform)

X_test_transform.columns = X_test_transform.columns.map(str)

model.fit(X_test_transform , y_test)
y_pred_test = model.predict(X_test_transform)
skplot.metrics.plot_confusion_matrix(y_test , y_pred_test)
# %%

a = skplot.metrics.roc_curve(y_train , model.predict_proba(X_train_transform)[:, 1])
b = skplot.metrics.auc(x=X_test_transform , y=y_pred_test)

print(f"Treino : {a} , Teste : {b}")
# %%
