'''
Código otimizado
'''
#%%
import pandas as pd
import sqlalchemy as sql

from feature_engine import selection
from feature_engine import imputation
from feature_engine import encoding

from sklearn import model_selection
from sklearn import pipeline

from sklearn import tree
from sklearn import ensemble

from sklearn import metrics
import scikitplot as skplot

# Import para criar as classes
from sklearn.base import BaseEstimator , TransformerMixin, clone
from typing import Optional , List , Literal
#%%

# Criação de classes personalizadas para compatibilizar com PIPELINE (Fit/Transform)

class ColumnNameCaster(BaseEstimator , TransformerMixin):
    """
    Classe para aplicar o MAP nos nomes dos campos que vem do SQLite como 'quoted'.
    
    A classe foi necessária devido o código ter quebrado ao manter os nomes de algumas colunas 
    como 'quoted, então vamos garantir, em pipeline, que essa transformação aconteça.

    Ele recebe:
        - X : dataframe com os campos nos formatos diversos
    
    No fit:
        - Apenas retorna a classe, pois não há aprendizado a ser efetuado
    
    No transform>
        - Aplica a regra de converter o NOME de cada coluna para STR
    """

    def fit(self, X, y=None):
        """Método para fazer o fit dos dados, apenas recebe a própria classe transformada"""
        return self

    def transform(self, X:pd.DataFrame):
        """Método para transformar efetivamente os dados"""
        X_ = X.copy()
        # Para cada nome de coluna, aplique um str(coluna)
        X_.columns = X_.columns.map(str)
        return X_

class ColumnTypeCaster(BaseEstimator , TransformerMixin):
    """
    Classe para aplicar as conversões de campos usando o dicionário.

    A classe serve para integrar o dataframe de dicionário da ABT para disparar eventos aos campos
    e nesse caso, vamos usar a coluna 'tipo_campo' para converter cada campo ao seu formato final,
    ainda que ele venha corretamente tipado do SQLite.

    A motivação foi que ao importar os dados, o sqlalchemy não está fazendo a tipagem de todos os
    campos corretamente, tendo sido necessário executar isso manualmente nas versões anteriores do
    código.

    Ele recebe:
        - mapping : dataframe com as informações sobre os campos, com a coluna 'tipo_campo'
    
    No fit:
        - Apenas retorna a classe, pois não há aprendizado a ser efetuado
    
    No transform>
        - Aplica a regra de converter cada coluna mapeada de acordo com o dicionario da ABT
    """

    def __init__(self, mapping:pd.DataFrame):
        """Capturar o dicionario como DataFrame na inicialização"""
        self.mapping = mapping
    
    def fit(self, X, y=None):
        """Retornar o objeto pós fit"""
        return self

    def transform(self , X:pd.DataFrame):
        """Método para aplicar as transformações de colunas usando o dicionário"""
        X_ = X.copy()
        
        mapa_tipos = self.mapping['tipo_campo'].to_dict()
        
        for chave , valor in mapa_tipos.items():
            try:
                X_[chave] = X_[chave].astype(valor)
            # se a coluna foi removida anteriormente, passar adiante
            except KeyError:
                continue
            except TypeError or ValueError:
                raise ValueError(
                    f"Não foi possível converter o campo {chave} para tipo {valor}\n"
                    f"Veriricar os campos e tentar novamente"
                )
        
        return X_

class DynamicColumnSelector(BaseEstimator , TransformerMixin):
    """
    Wrapper que transforma qualquer transformer colunar (feature_engine, imputs) em um transformer
    mais robusto que trata colunas ausentes. No nosso pipe essa etapa vem após a remoção de colunas
    do DF original usando diferenças entre as médias ou medianas entre grupos.

    Essa classe recebe:
        - transformer : um objeto tipo sklearn (sklearn-like) que ACEITA PARAMETRO "variables", ou
        seja variables=[...]
        - variables : a lista "completa" de colunas, mesmo com informações de colunas que possam
        vir a ser removidas no pipeline
        - step_pipe : nome da etapa do pipeline onde está sendo usada
        - on_empty : ação a ser executada caso tenhamos como resultado um DF vazio
    
    No fit:
        - Calcula a interseção com X.columns (que é o resultado da etapa anterior do pipeline, ex.
        DROP)
        - clona o transformer original configurando apenas as colunas que são ainda existentes
        - faz o fit do CLONE
    
    No transform:
        - aplica o transformer já "fitado", ou seja, ensinamos ao transformer o que ele deve aprender
        no fit e usamos essa etapa para aplicar

    """

    def __init__(self, transformer , variables: Optional[List[str]] = None ,
                 * , step_pipe: str = "Dynamic Selector" , 
                     on_empty: Literal['stop' , 'pass'] = 'pass'):
        
        self.transformer = transformer
        self.variables = variables or []
        self.step_pipe = step_pipe
        self.on_empty = on_empty

        # Flag que modela o comportamento de NO-OP
        self.is_no_op = False
        
        # Validar o formato on_empty
        if on_empty not in {'stop', 'pass'}:
            raise ValueError(
                f"[{self.step_pipe}] Ação inválida em on_empty.\n"
                f"Escolha uma entre 'pass' para etapas não críticas ou 'stop' para etapas críticas"
            )
        
    
    def fit(self, X:pd.DataFrame, y=None):
        X_ = X.copy()

        # Inner Join dinâmico (o que fazia usando intersection com X_train)
        self.variables_ = [col for col in self.variables if col in X_.columns]

        # Alterar a flag de NO-OP para os casos de não ter coluna alguma
        self.is_no_op_ = (len(self.variables_) == 0)

        # Se deu ruim na captura das colunas, verifica o tipo de ação fornecida
        if self.is_no_op_:
        
            # Se o usuario escolheu STOP, vamos parar a execução
            if self.on_empty == 'stop':
                
                expected_preview = self.variables[:15]
                available_preview = X_.columns[:15].to_list()
                raise ValueError(
                    f"[{self.step_pipe}] Nenhuma coluna encontrada para aplicar o transformer.\n"
                    f"Qtde de colunas esperadas (total) : {len(self.variables)}\n"
                    f"Exemplo de colunas esperadas (até 15) : {expected_preview}\n"
                    f"Qtde de colunas disponíveis (total) : {len(X_.shape[1])}\n"
                    f"Exemplo de colunas esperadas (até 15) : {available_preview}\n"
                    f"Dica: verifique se DROP removeu todas as colunas desta regra\n"
                    f"ou se o dicionário da ABT está desatualizado para schema atual."
                )

            # Se o usuario escolheu PASS...
            elif self.on_empty == 'pass':
                
                # ... não configuramos transformer_ nem fit, pois serão ignoradas
                self.transformer_ = None
                return self

            else:

                raise ValueError(
                    f"[{self.step_pipe}] Ação inválida em on_empty.\n"
                    f"Escolha uma entre 'pass' para etapas não críticas ou 'stop' para etapas críticas"
                )

        # Caso contrario, segue com o fluxo
        else:

            # Clonar o transformer e injetar as novas variaveis
            self.transformer_ = clone(self.transformer)

            # Procurar o parametro "variables". Caso não exista no transformer incluir os casos
            # nesse trecho
            if "variables" in self.transformer_.get_params():
                self.transformer_.set_params(variables=self.variables_)
        
            # Agora rodamos o fit no transformer pronto
            self.transformer_.fit(X_, y)
        
            return self

    def transform(self , X:pd.DataFrame):
        X_ = X.copy()

        if self.is_no_op_:
            return X_
        else:
            return self.transformer_.transform(X_)


class dic_ABT():
    """
    Cria um dicionário para documentação da ABT que tem alguns campos padrão para reuso em etapas
    subsequentes do código:

    Ele recebe:
        - X_train : um DF com as features que estão vindo da fonte
    
    Ele devolve:
        - Um dataframe com as colunas:
            1. feature : o nome da feature capturado do DF
            2. pctNulos : o percentual de nulos da coluna (numéricos e strings)
            3. temNulo : bool que indica se o campo tem ao menos um valor nulo
            4. action_fill : descrição amigavel de o que fazer com esse campo nas subetapas do
            MODIFY que será executado
            5. notas : anotações uteis para justificar o action_fill
            6. etapa_pipe : para facilitar, podemos já montar as etapas do pipeline que estamos
            enxergando para usar posteriormente
            7. tipo_campo : tipagem que o campo deveria ter
    """
    
    def __init__(self, X_train:pd.DataFrame):
        """Instanciar o DF como membro da classe"""
        if X_train is None:
            print("Data Frame de X_train não foi fornecido")
            return None

        else:

            X_train_ = X_train.copy()

            self.df = (
                pd.DataFrame(data=X_train_.isna().mean())     # Calcular a taxa de nulos
                .reset_index()                                            
                .rename(columns={"index":"feature" ,          # Renomear colunas
                                0 : "pctNulos"})
                .assign(
                    temNulo = lambda x : x['pctNulos'] > 0 ,  
                    action_fill = None ,                      
                    notas = None ,                            
                    etapa_pipe = None ,                       
                    tipo_campo = None                         
                    )
                .set_index(keys=['feature'])                  # Setar indice para facilitar gestão
            )
    
    def update(self, dic:dict=None):
        """Atualizar o DF da classe"""
        if dic is None:
            raise ValueError(
                f"Forneça um dicionário válido para preenchimento"
            )

        for chave, item in dic.items():
            try:
                self.df.loc[item['features'] , 'action_fill'] = item['action_fill']
                self.df.loc[item['features'] , 'notas'] = item['notas']
                self.df.loc[item['features'] , 'etapa_pipe'] = item['etapa_pipe']
                self.df.loc[item['features'] , 'tipo_campo'] = item['tipo_campo']
            except KeyError as err:
                print(
                    f"A chave {chave} tem valores de itens inválidos"
                )

            

# %%
# Conectar com a base de dados

conn = sql.create_engine('sqlite:///../../data/analytics/database.db')

#%%

# SAMPLE - Import dos dados

df = pd.read_sql('abt_flFiel' , conn)
df.head()
# %%

# SAMPLE - OOT e ABT

target = 'flFiel'
features = df.columns.to_list()[4:]

df_oot = df[df['tpDado']=='OOT'].reset_index(drop=True)
df_abt = df[df['tpDado']=='ABT'].reset_index(drop=True)

X = df_abt[features]
y = df_abt[target]

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

# Criar o nosso dicionário de documentação de ABT
dic_Features = dic_ABT(X_train)

# %%

# EXPLORE (somente Base de Treino) - MISSING P2

# Features categoricas
features_categoricas = ['descLifeCycleAtual', 'descLifeCycleD28']

# Features de cursos
features_float_cursos = [
    'Carreira', 'ColetaDados2024', 'DataPlatform2025', 'DsDatabricks2024',
    'DsPontos2024', 'Estatistica2024', 'Estatistica2025', 'GitHub2024',
    'GitHub2025', 'Go2026', 'IaCanal2025', 'LagoMago2024',
    'LoyaltyPredict2025', 'MachineLearning2025', 'MatchmakingTramparDeCasa2024', 'Ml2024',
    'MlFlow2025', 'Nekt2025', 'Pandas2024', 'Pandas2025',
    'Python2024', 'Python2025', 'SpeedF1', 'SQL2020',
    'SQL2025', 'Streamlit2025', 'TramparLakehouse2024', 'TSEAnalytics2024',
 ]

# Features numericas inteiras para zero
features_int_tozero = [
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
features_float_tozero = [
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
features_float_to1000 = [
    'avgIntervaloDiasVida', 'avgFreqGrupo' , 'avgIntervaloDiasD28' ,
    'avgFreqD7', 'avgFreqD14' , 'qtdeDiasUltAtividade'
]

# Features que provocariam a exclusão da amostra
features_int_exclude = [ 'idadeDias' ]

# Composição do dicionario
dic_dicionario = {
    "feat_categoricas" : {
        "features" : features_categoricas ,
        "action_fill" : "Fill MISSING" ,
        "notas" : "Indica que o usuário não existia no recorte",
        "etapa_pipe" : "fill_missing" ,
        "tipo_campo" : "str"
    } ,
    "feat_cursos" : {
        "features" : features_float_cursos ,
        "action_fill" : "ZERAR" ,
        "notas" : "0 indica que o curso não foi feito",
        "etapa_pipe" : "fill_cursos" ,
        "tipo_campo" : "float"
    } ,
    "feat_int_tozero" : {
        "features" : features_int_tozero ,
        "action_fill" : "ZERAR" ,
        "notas" : "Features Maior-Melhor. 0 indica pior cenário",
        "etapa_pipe" : "fill_tozero" ,
        "tipo_campo" : "Int64"
    } ,
    "feat_float_tozero" : {
        "features" : features_float_tozero ,
        "action_fill" : "ZERAR" ,
        "notas" : "Features Maior-Melhor. 0 indica pior cenário",
        "etapa_pipe" : "fill_tozero" ,
        "tipo_campo" : "float"
    } ,
    "feat_float_to1000" : {
        "features" : features_float_to1000 ,
        "action_fill" : "Imputar 1000" ,
        "notas" : "Features Menor-Melhor. 1000 indica pior cenário",
        "etapa_pipe" : "fill_to1000" ,
        "tipo_campo" : "float"
    } ,
    "feat_int_exclude" : {
        "features" : features_int_exclude ,
        "action_fill" : "Excluir amostra" ,
        "notas" : "Erro de calculo de idade indica erro critico da amostra",
        "etapa_pipe" : "remove_sample" ,
        "tipo_campo" : "Int64"
    }
}


# %%

# Atualização do dicionario
dic_Features.update(dic_dicionario)

# Atualizacao das listas
features_numericas = [feat for feat in features if feat not in features_categoricas]

condicao_print = dic_Features.df['etapa_pipe'].isna()
print(dic_Features.df[condicao_print].index)

#%%

# EXPLORE (somente Base de Treino) - ANÁLISE BIVARIADA P1:Numerica 
# (Covariaveis vs Variavel)

df_train = X_train.copy()
df_train[target] = y_train.copy()

# Converter campos
df_train[target] = df_train[target].astype(int)

mapa_tipos = dic_Features.df['tipo_campo'].to_dict()
for chave , valor in mapa_tipos.items():
    df_train[chave] = df_train[chave].astype(valor)

bivar_num = df_train.groupby(target)[features_numericas].median().T
print(bivar_num.to_string())


bivar_num['ratio'] = (bivar_num[1] + 0.001) / (bivar_num[0] + 0.001)
print(bivar_num.sort_values(by='ratio', ascending=False).to_string())

regra_remoção = (bivar_num['ratio'] == 1) | (bivar_num['ratio'].isna())
to_remove = bivar_num[regra_remoção].index.tolist()

#%%

# EXPLORE (somente Base de Treino) - ANÁLISE BIVARIADA P2:Categorica 

for i in features_categoricas:
    print(df_train.groupby(i)[target].mean().T)
    print("\n")

# %%

# MODIFY - Ajustar nomes de campos (Usando a classe para implementar FIT e TRANSFORM, 
# essenciais para o PIPE)
pipe_ColumnNameCaster = ColumnNameCaster()

# %%

# MODIFY - Converter formatos de campos
pipe_CastColumns = ColumnTypeCaster(dic_Features.df)

#%%

# MODIFY - DROP (Tirar variaveis irrelevantes)
regra_remoção = (bivar_num['ratio'] == 1) | (bivar_num['ratio'].isna())
to_remove = bivar_num[regra_remoção].index.tolist()
pipe_drop_features = selection.DropFeatures(to_remove)

# %%

# MODIFY - IMPUTATION (Definir Regras)

# Zerar os numericos aptos que estejam na base pós DROP
etapa_1 = "Preencher zeros"
cols_fill_tozero = dic_Features.df.query("etapa_pipe == 'fill_tozero'").index.to_list()
imput_fill_tozero = DynamicColumnSelector(
    transformer=imputation.ArbitraryNumberImputer(arbitrary_number=0) ,
    variables=cols_fill_tozero , step_pipe=etapa_1 , on_empty='stop'
)

# Aumentar os valores de features menor-melhor que estejam na base pós DROP
etapa_2 = "Preencher 1000"
cols_fill_to1000 = dic_Features.df.query("etapa_pipe == 'fill_to1000'").index.to_list()
imput_fill_to1000 = DynamicColumnSelector(
    transformer=imputation.ArbitraryNumberImputer(arbitrary_number=1000),
    variables=cols_fill_to1000 , step_pipe=etapa_2 , on_empty='stop'
)

# Colunas de cursos com 0 quando não fez que estejam na base pós DROP
etapa_3 = "Preencher cursos 0"
cols_fill_cursos = dic_Features.df.query("etapa_pipe == 'fill_cursos'").index.to_list()
imput_fill_cursos = DynamicColumnSelector(
    transformer=imputation.ArbitraryNumberImputer(arbitrary_number=0),
    variables=cols_fill_cursos , step_pipe=etapa_3 , on_empty='stop'
)

# Pessoas que não existiam 28 dias atrás
etapa_4 = "Preencher não-usuários"
cols_fill_missing = dic_Features.df.query("etapa_pipe == 'fill_missing'").index.to_list()
imput_fill_missing = DynamicColumnSelector(
    transformer=imputation.CategoricalImputer(fill_value='Não-usuário') ,
    variables=cols_fill_missing , step_pipe=etapa_4 , on_empty='stop'
)

#%%

# MODIFY - ONE HOT ENNCODER
etapa_5 = "OneHot Encoding"
cols_one_hot = dic_Features.df.query("tipo_campo == 'str'").index.to_list()
imput_onehot = DynamicColumnSelector(
    transformer=encoding.OneHotEncoder() ,
    variables=cols_one_hot , step_pipe=etapa_5 , on_empty='stop'
)

# %%

# ASSESS

# model = tree.DecisionTreeClassifier(random_state=42, 
#                                     min_samples_leaf=30)

model = ensemble.RandomForestClassifier(n_estimators=150 ,
                                        min_samples_leaf=30,
                                        n_jobs=-1)

# model = ensemble.AdaBoostClassifier(
#     random_state=42,
#     n_estimators=150,
#     learning_rate=0.1
# )

# %%

# MODIFY - IMPUTATION (Pipeline)
model_pipeline = pipeline.Pipeline(steps=[
    ("Cast de Colunas" , pipe_ColumnNameCaster) ,
    ("Cast de Campos" , pipe_CastColumns) ,
    ("Drop de features irrelevantes" , pipe_drop_features) ,
    (etapa_1 , imput_fill_tozero) ,
    (etapa_2 , imput_fill_to1000) ,
    (etapa_3 , imput_fill_cursos) ,
    (etapa_4 , imput_fill_missing) ,
    (etapa_5 , imput_onehot) ,
    ("Algoritmo" , model)
])

# %%

model_pipeline.fit(X_train , y_train)

y_train_pred = model_pipeline.predict(X_train)
y_train_proba = model_pipeline.predict_proba(X_train)

y_test_pred = model_pipeline.predict(X_test)
y_test_proba = model_pipeline.predict_proba(X_test)

X_oot = df_oot[features]
y_oot = df_oot[target]

y_oot_pred = model_pipeline.predict(X_oot)
y_oot_proba = model_pipeline.predict_proba(X_oot)

# %%

# ASSESS - Metricas

# Acuracias

acc_train = metrics.accuracy_score(y_train , y_train_pred)
acc_test = metrics.accuracy_score(y_test , y_test_pred)
acc_oot = metrics.accuracy_score(y_oot , y_oot_pred)

print(
    f"Acurácias obervadas :\n"
    f"Treino : {acc_train}\n"
    f"Teste : {acc_test}\n"
    f"OOT : {acc_oot}"
)

# AUCs

auc_train = metrics.roc_auc_score(y_train , y_train_proba[:,1])
auc_test = metrics.roc_auc_score(y_test , y_test_proba[:,1])
auc_oot = metrics.roc_auc_score(y_oot , y_oot_proba[:,1])

print(
    f"AUCs obervados :\n"
    f"Treino : {auc_train}\n"
    f"Teste : {auc_test}\n"
    f"OOT : {auc_oot}"
)
# %%
'''
            Acurácias                   AUCs
Treino :    0.9464346639372228          0.9506400340930614
Teste :     0.9304229195088677          0.8113589498566613
OOT :       0.7817047817047817          0.8219648372535043

O que observar:

1. Overfitting : AUC muito alto em Treino e muito baixo em Teste e OOT
2. Data Leakage : AUC muito alto em todas as bases

    > Variável vazada é quando usamos uma covariável que contem a resposta/causa da variavel,
    >> Usar Tempo de Prova para prever o Vencedor de uma corrida (menor tempo é a resposta)
    >> Usar n de ligações para cancelamento para prever Cancelamento (todo mundo tem que ligar)
    > Para testar leakage podemos olhar as importancias das features no modelo, procurando por
    alguma com valor extremamente alto (~70%)

'''
#%%

# Rodar as features importances

# Capturar os nomes das variaveis que ficaram referenciando até a penultima etapa (OneHot)
# e aplicando o método TRANSFORM
feat_names_ = model_pipeline[:-1].transform(X_train).columns.tolist()

feat_importances = pd.Series(data=model_pipeline[-1].feature_importances_ , 
                             index=feat_names_)
feat_importances.sort_values(ascending=False)
# %%

# ASSESS - Persistir modelo

model_series = pd.Series({
    "model" : model_pipeline ,
    "raw_features" : features ,
    "features" : model_pipeline[:-1].transform(X_train).columns.tolist() ,
    "auc_train" : auc_train ,
    "auc_test" : auc_test ,
    "auc_oot" : auc_oot
})

model_series.to_pickle('13a.model_fiel.pkl')
# %%
