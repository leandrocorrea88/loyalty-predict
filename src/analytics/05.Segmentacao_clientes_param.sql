/*
Esse arquivo é dedicado a consolidar as  fontes de segmentação de usuário na nossa base de ativos,
usando as seguinte fontes:

(03a.Life_Cycle_param.sql) Consolida e parametriza as consultas de RECENCIA dos usuários

(04c.Frequencia_Valor_cluster.sql) Consolida os cluster formados a partir do cruzamento entre as
dimensões de FREQUENCIA e VALOR

A ideia dessa etapa é consolidar ambas em um unico arquivo, parametrizável para que possamos então
visualizar todas as classificações sobre os usuários da plataforma TMW
*/

WITH
tb_daily AS (
-- Calcular usuários no DAU em base diária
    SELECT  
        DISTINCT
            IdCliente ,
            DATE(DtCriacao) AS dtDia
    FROM transacoes
    -- Inserimos aqui uma variável para fazer o loop e tirar as fotos em cada mes. Usamos o simbolo 
    -- < e não <= para poder pegar as transações que ocorreram até as 23h59 do dia anterior
    -- Inserimos aqui o parâmetro para ser consumido pelo Python
    WHERE DtCriacao < '{_date}'
) ,

tb_Idade AS(
-- Calcular a primeira e ultima transações de cada usuário
    SELECT
        IdCliente ,
        -- Essas linha servem para validação, apenas
        -- MIN(dtDia) AS dtPrimTransacao ,
        -- MAX(dtDia) AS dtUltTransacao ,
        -- Calculamos o MAX/MIN porque a linha anterior faz o calculo por transação (por DAU)
        CAST(MAX(JULIANDAY('{_date}') - JULIANDAY(dtDia)) AS INT) AS qtdeDiasPrimTransacao ,
        CAST(MIN(JULIANDAY('{_date}') - JULIANDAY(dtDia)) AS INT) AS qtdeDiasUltTransacao

    FROM
        tb_daily
    GROUP BY
        IdCliente
) ,

tb_rn AS(
-- Listar as transações (dailies) dos usuários ordendas de maneira descrescente
    SELECT 
        * ,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY dtDia DESC) AS rnDia
    FROM tb_daily
),

tb_penultima_ativacao AS (
-- Capturar a penultima ativação de cada usuário.
    SELECT 
        * ,
        CAST(JULIANDAY('{_date}') - JULIANDAY(dtDia) AS INT) AS qtdeDiasPenultTransacao
    FROM tb_rn 
    WHERE rnDia = 2
) ,

tb_life_cycle AS (
    SELECT
        t1.* ,
        -- Usuários que só apareceram uma vez terão esse campo VAZIO. Um teste pra isso é verificar
        -- que essas ocorrências tem qtdeDiasPrimTransacao = qtdeDiasUltTransacao
        t2.qtdeDiasPenultTransacao ,
        -- Classificar os status
        CASE
            WHEN qtdeDiasPrimTransacao <= 7 THEN '01-CURIOSO'
            WHEN qtdeDiasUltTransacao <= 7 AND (qtdeDiasPenultTransacao - qtdeDiasUltTransacao) <= 14 THEN '02-FIEL'
            WHEN qtdeDiasUltTransacao <= 7 AND (qtdeDiasPenultTransacao - qtdeDiasUltTransacao) BETWEEN 15 and 27 THEN '02-RECONQUISTADO'
            WHEN qtdeDiasUltTransacao <= 7 AND (qtdeDiasPenultTransacao - qtdeDiasUltTransacao) >= 28 THEN '02-RENASCIDO'
            WHEN qtdeDiasUltTransacao BETWEEN 8 AND 14 THEN '03-TURISTA'
            WHEN qtdeDiasUltTransacao BETWEEN 15 AND 28 THEN '04-DESENCANTADO'
            WHEN qtdeDiasUltTransacao > 28 THEN '05-ZUMBI'
        END AS descLifeCycle 
    FROM
        tb_idade AS t1
        LEFT JOIN tb_penultima_ativacao AS t2
            ON t1.IdCliente = t2.idCliente
) ,

tb_freq_valor AS(
    SELECT DISTINCT
        IdCliente , 
        COUNT(DISTINCT DATE(DtCriacao)) AS qtdeFrequencia ,
        SUM(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosPositivos ,
        SUM(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE ABS(QtdePontos) END) AS qtdePontosTotais ,
        SUM(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosGanho ,
        SUM(CASE WHEN QtdePontos < 0 THEN ABS(QtdePontos) ELSE 0 END) AS qtdePontosTroca
    FROM transacoes
    WHERE 
        DtCriacao < '{_date}'
        AND DtCriacao > DATE('{_date}' , '-28 DAYS')
        --AND IdCliente = 'a327611d-a675-4f90-bce8-5aef803458a8'
    GROUP BY IdCliente
    ORDER BY DtCriacao DESC
) ,

tb_cluster AS(
    SELECT 
        * ,
        -- Vamos atribuir o código do cliente baseado na sua posição nos quadrantes (Valor, Freq)
        -- pois assim podemos ter uma ideia de priorização entre os grupos.
        -- UPDATE : Os codigos foram definidos de acordo com a visão de priorização do NEGOCIO (TMW)
        CASE 
            -- Usuários que VEM MUITO POUCO, e GASTAM POUCO (Val=0, Freq=0)
            WHEN qtdeFrequencia < 5 AND qtdePontosPositivos < 900 THEN '00.LURKER'
            -- Usuários que VEM POUCO, e GASTAM POUCO (Val=0, Freq=1)
            WHEN qtdeFrequencia BETWEEN 5 AND 10 AND qtdePontosPositivos < 900 THEN '01.PREGUIÇOSO'
            -- Usuários que VEM BASTANTE, e GASTAM POUCO (Val=0, Freq=2)
            WHEN qtdeFrequencia >= 10 AND qtdePontosPositivos < 900 THEN '20.POTENCIAL'
            
            -- Usuários que VEM POUCO, mas GASTAM MEDIO (Val=1, Freq=0)
            WHEN qtdeFrequencia < 10 AND qtdePontosPositivos BETWEEN 900 AND 1500 THEN '11.INDECISO'
            -- Usuários que VEM BASTANTE, mas GASTAM MEDIO (Val=1, Freq=1)
            WHEN qtdeFrequencia >= 10 AND qtdePontosPositivos BETWEEN 900 AND 1500 THEN '21.ESFORÇADO'
            
            -- Usuários que VEM POUCO, mas GASTAM MUITO (Val=2, Freq=1)
            WHEN qtdeFrequencia < 10 AND qtdePontosPositivos > 1500 THEN '12.HYPER'
            -- Usuários que VEM BASTANTE e GASTAM MUITO (Val=2, Freq=2)
            WHEN qtdeFrequencia >= 10 AND qtdePontosPositivos > 1500 THEN '22.EFICIENTE'
        END AS cluster
    FROM tb_freq_valor
)

-- Agora fechando a visão consolidada
SELECT
    -- Botar a marcação da data de corte
    -- Inserimos aqui o parâmetro para ser consumido pelo Python
    DATE('{_date}' , '-1 DAY') as dtRef,
    t1.* ,
    t2.qtdeFrequencia ,
    t2.qtdePontosPositivos ,
    t2.qtdePontosGanho ,
    t2.qtdePontosTroca ,
    t2.cluster
FROM
    tb_life_cycle AS t1
    LEFT JOIN tb_cluster AS t2
        ON t1.IdCliente = t2.IdCliente