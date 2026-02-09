'''
Diferente da Recencia que pudemos arbitrar valores para as classes, com a Frequencia e Valor a 
tarefa se torna mais difícil. Então aqui vamos usar uma tecnica de agrupamento para entender
quais linhas de corte podemos usar para criar as classes dessas duas novas dimensões
'''
#%%

import pandas as pd
import sqlalchemy as sql
import matplotlib.pyplot as plt
import seaborn as sns

#%%

# Conectar ao banco de dados
string_conexao = 'sqlite:///' + '../../data/loyalty-system/database.db'
engine = sql.create_engine(string_conexao)

# %%

# Criar uma função para ler o arquivo SQL
def import_sql(path):
    with open(file=path, mode='r') as open_file:
        query = open_file.read()
    return query

#%%

# Executar o comando SQL no Banco de Dados
query = import_sql('04.Frequencia_Valor.sql')
df = pd.read_sql(sql=query , con=engine)

# %%

# Plotar os resultados
plt.plot(df['qtdeFrequencia'], df['qtdePontosPositivos'], 'o')
plt.grid(visible=True)
plt.xlabel("Frequencia")
plt.ylabel("Valor")
plt.show()

'''
Em alguns momentos do passado houve um bug que beneficiava o usuário 'cajuuh', recebendo os pontos
de outros usuários que estavam trocando seus pontos. Como não tem como fazer essa correção de maneira
retroativa vamos excluir esse usuário da análise

Após essa consideração, vamos aplicar o algoritmo de clusterização usando o SciKit-Learn
'''
# %%

from sklearn import cluster

kmean = cluster.KMeans(n_clusters=5 , random_state=42 ,
                       max_iter=1000)

kmean.fit(df[['qtdeFrequencia', 'qtdePontosPositivos']])

df['cluster'] = kmean.labels_

# %%

df.groupby(by='cluster').describe()

sns.scatterplot(data=df , x='qtdeFrequencia' , y='qtdePontosPositivos' ,
                hue='cluster' , palette='deep')
plt.grid(visible=True, linestyle='--')
plt.show()

'''
Ao visualizar os resultados vemos que 

- Existe uma grande concentração de dados no primeiro grupo
- As escalas entre Frequencia e Valor são muito distantes
- Há presença de outliers que são frutos de BUG de sistemas

Então para a nossa segunda versão de Clustering vamos

- Padronizar as distribuições
- Remover os usuários bugados

'''
# %%

from sklearn import preprocessing

# Removendo pontuações exageradas
df_corrigido = df[df['qtdePontosPositivos'] < 4000]

minmax = preprocessing.MinMaxScaler()

X = minmax.fit_transform(df_corrigido[['qtdeFrequencia', 'qtdePontosPositivos']])

kmean = cluster.KMeans(n_clusters=5 ,
                       random_state=42 ,
                       max_iter=1000)

kmean.fit(X)

df_corrigido['cluster'] = kmean.labels_


# Conferinndo os novos totais
df_corrigido.groupby(by='cluster')['IdCliente'].count()

# Para plotar os dados, fazemos da maneira NAO NORMALIZADA para ter interpretação
sns.scatterplot(data=df_corrigido , x='qtdeFrequencia' , y='qtdePontosPositivos' ,
                hue='cluster' , palette='deep')
plt.grid(visible=True, linestyle='--')
plt.show()

'''
Agora temos os grupos agregados usando uma escala padronizada, mas a explicação desse resultado
para uma área de negócio é dificil, então podemos usar como base para estabelecer nossos criterios
'''
# %%

# Manter os dados não padronizados para plotagem
sns.scatterplot(data=df_corrigido , x='qtdeFrequencia' , y='qtdePontosPositivos' ,
                hue='cluster' , palette='deep')
# Agora podemos traçar retas verticais e horizontais baseadas na distribuição dos pontos
plt.hlines(y=1500 , xmin=0 , xmax=26 , colors='black')
plt.hlines(y=900 , xmin=0 , xmax=26 , colors='black')
plt.vlines(x=10 , ymin=0 , ymax=3000, colors='black')
plt.vlines(x=5 , ymin=0 , ymax=900, colors='black')
#plt.grid(visible=True, linestyle='--')
plt.show()

'''
Usando o algoritmo de cluster como ponto de partida, definimos as regras para 7 clusters de usuários
e agora vamos devolver essa classificação à query de frequencia e valor usando as regras definidas
em uma nova versão
'''
# %%

'''
Para calcular visualmente a quantidade otima de clusters podemos usar a métrica de INERCIA iterando
em uma quantidade de hipoteses que queremos testar em n_estimators (clusters)
'''
cluster_range = range(1, 7)
inertias = []

for k in cluster_range:
    kmean = cluster.KMeans(n_clusters=k , random_state=42 , max_iter=1000)
    kmean.fit(X)
    inertias.append(kmean.inertia_)

plt.plot(cluster_range , inertias , marker='o')
plt.title("Método Elbow")
plt.xlabel("Numero de clusters")
plt.ylabel("Inercia (WCSS)")
plt.grid(True)
plt.show()
# %%

'''
Outra forma mais direta de fazer esse calculo é usando SciKit Plot, que já tem esse método disponivel
para uso

Em linhas gerais, o Elbow Curve responde a perguntas como "vale a pena aumentar a complexidade 
da minha segmentação? o que ganho com isso?" e faz iss através da soma minimos quadrados das
distâncias dos pontos ao ponto médio do seu cluster  
'''

import scikitplot as skplot

skplot.cluster.plot_elbow_curve(clf=kmean , X=X , cluster_ranges=range(1,11))
skplot.cluster.print_function
# %%

'''
Existem outras métricas que podem ajudar a definir, como
- Silhouette Score
- Calinski-Harabasz (CH)
- Davies-Bouldin (DB)
'''

from sklearn import metrics

# %%

# Silhouette Score

cluster_range = range(2, 11)    # Silhouette nao roda para k=2
scores = []

for k in cluster_range:
    kmean = cluster.KMeans(n_clusters=k , random_state=42 , max_iter=1000)
    kmean.fit(X)
    labels = kmean.fit_predict(X)
    score = metrics.silhouette_score(X , labels)
    scores.append(score)

plt.plot(cluster_range , scores , marker='o')
plt.title("Silhouette Score vs k")
plt.xlabel("Numero de clusters")
plt.ylabel("Silhouette Score")
plt.grid(True)
plt.show()

# %%

# Calinski-Harabasz (Variance Ratio Criterion)

cluster_range = range(2, 11)    # Silhouette nao roda para k=2
ch_scores = []

for k in cluster_range:
    kmean = cluster.KMeans(n_clusters=k , random_state=42 , max_iter=1000)
    kmean.fit(X)
    labels = kmean.fit_predict(X)
    score = metrics.calinski_harabasz_score(X , labels)
    ch_scores.append(score)

plt.plot(cluster_range , ch_scores , marker='o')
plt.title("Calinski-Harabasz vs k")
plt.xlabel("Numero de clusters")
plt.ylabel("Calinski-Harabasz Score")
plt.grid(True)
plt.show()

# %%

# Davies-Bouldin

cluster_range = range(2, 11)    # Silhouette nao roda para k=2
db_scores = []

for k in cluster_range:
    kmean = cluster.KMeans(n_clusters=k , random_state=42 , max_iter=1000)
    kmean.fit(X)
    labels = kmean.fit_predict(X)
    score = metrics.davies_bouldin_score(X , labels)
    db_scores.append(score)

plt.plot(cluster_range , db_scores , marker='o')
plt.title("Davies-Bouldin vs k")
plt.xlabel("Numero de clusters")
plt.ylabel("Davies-Bouldin Score")
plt.grid(True)
plt.show()
# %%

'''
Vimos que existem várias métricas para avaliar uma segmentação, cada uma com um foco:

1. Elbow : Identifica onde o ganho marginal diminui (sugere COMPLEXIDADE)
2. Silhouette : Confirma se há separação real (avalia SEPARAÇÃO)
3. CH e DB : Validam compactação e sobreposição (validam a ESTRUTURA)

Mas no fim, o negócio decide em função de OPERACIONALIZAÇÃO E RUIDO, ou seja, o que é POSSÍVEL DE
GERENCIAR no fim do dia!

Importante ter uma noção de que as vezes o DADO NÃO ACEITA CLUSTER. Isso pode acontecer quand, ao 
mesmo tempo encontramos:

1. Se elbow não é claro
2. Silhouette é < 0.3
3. DB alto

Resumão de cluster

| Métrica                          | O que mede                              | Quando usar                                   | Como interpretar                                       | Armadilha comum                        |
| -------------------------------- | --------------------------------------- | --------------------------------------------- | ------------------------------------------------------ | -------------------------------------- |
| **Elbow / Inertia (SSE / WCSS)** | Compactação interna dos clusters        | **Primeiro filtro** para limitar complexidade | Queda forte → ganho real; curva plana → ganho marginal | Sempre diminui → não “escolhe” sozinha |
| **Silhouette Score**             | Separação vs coesão de cada ponto       | Avaliar **qualidade real** da segmentação     | Varia de −1 a 1; quanto maior, melhor                  | Pode favorecer poucos clusters         |
| **Calinski–Harabasz (CH)**       | Razão variância entre / dentro clusters | Confirmar equilíbrio estrutura × compactação  | Quanto maior, melhor                                   | Cresce com k em alguns dados           |
| **Davies–Bouldin (DB)**          | Similaridade entre clusters             | Penalizar clusters sobrepostos                | Quanto **menor**, melhor                               | Sensível a escala                      |
| **Negócio (critério final)**     | Usabilidade e ação                      | Escolha final                                 | Segmentos acionáveis e estáveis                        | Ignorar restrições operacionais        |


'''