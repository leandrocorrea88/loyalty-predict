/*
                    Ultima trans.   Penultima trans.
                    (Recencia)      (Rec. Anterior)
                    -------------   ----------------
    Curiosa         <= 7d           (vazio)
    Fiel            <= 7d           < 15d
    Turista         <= 14d
    Desencantado    <= 28d
    Zumbi           > 28d
    Reconquistado   <= 7d           15d >= X >= 28d 
    Renascido       <= 7d           > 28d

Essa query é a copia da anterior, com a DATA como PARAMETRO

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
            WHEN qtdeDiasUltTransacao >= 28 THEN '05-ZUMBI'
        END AS descLifeCycle 
    FROM
        tb_idade AS t1
        LEFT JOIN tb_penultima_ativacao AS t2
            ON t1.IdCliente = t2.idCliente
)


SELECT
    -- Botar a marcação da data de corte
    -- Inserimos aqui o parâmetro para ser consumido pelo Python
    DATE('{_date}' , '-1 DAY') as dtRef,
    *
FROM
    tb_life_cycle

