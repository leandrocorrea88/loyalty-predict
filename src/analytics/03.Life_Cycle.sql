/*
Reguas de Idade na base

    Curiosa : Idade < 7d

Reguas de Recencia

                    Ultima trans.   Penultima trans.
                    (Recencia)      (Rec. Anterior)
                    -------------   ----------------
    Fiel            <= 7d           < 15d
    Turista         <= 14d
    Desencantado    <= 28d
    Zumbi           > 28d
    Reconquistado   <= 7d           15d >= X >= 28d 
    Renascido       <= 7d           > 28d
*/
WITH
tb_daily AS (
-- Calcular usuários no DAU em base diária
    SELECT  
        DISTINCT
            IdCliente ,
            DATE(DtCriacao) AS dtDia
    FROM transacoes
) ,

tb_Idade AS(
-- Calcular a primeira e ultima transações de cada usuário
    SELECT
        IdCliente ,
        -- Essas linha servem para validação, apenas
        -- MIN(dtDia) AS dtPrimTransacao ,
        -- MAX(dtDia) AS dtUltTransacao ,
        -- Calculamos o MAX/MIN porque a linha anterior faz o calculo por transação (por DAU)
        CAST(MAX(JULIANDAY('now') - JULIANDAY(dtDia)) AS INT) AS qtdeDiasPrimTransacao ,
        CAST(MIN(JULIANDAY('now') - JULIANDAY(dtDia)) AS INT) AS qtdeDiasUltTransacao

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
        CAST(JULIANDAY('now') - JULIANDAY(dtDia) AS INT) AS qtdeDiasPenultTransacao
    FROM tb_rn 
    WHERE rnDia = 2
) ,

tb_life_cycle AS (
    SELECT
        t1.* ,
        -- Usuários que só apareceram uma vez terão esse campo VAZIO. Um teste pra isso é verificar
        -- que essas ocorrências tem qtdeDiasPrimTransacao = qtdeDiasUltTransacao
        t2.qtdeDiasPenultTransacao ,
        -- Classificar
        CASE
            WHEN qtdeDiasPrimTransacao <= 7 THEN '01-CURIOSO'
            WHEN qtdeDiasUltTransacao <= 7 AND (qtdeDiasPenultTransacao - qtdeDiasUltTransacao) <= 14 THEN '02-FIEL'
            WHEN qtdeDiasUltTransacao BETWEEN 8 AND 14 THEN '03-TURISTA'
            WHEN qtdeDiasUltTransacao BETWEEN 15 AND 28 THEN '04-DESENCANTADO'
            WHEN qtdeDiasUltTransacao > 28 THEN '05-ZUMBI'
            WHEN qtdeDiasUltTransacao <= 7 AND (qtdeDiasPenultTransacao - qtdeDiasUltTransacao) BETWEEN 15 and 28 THEN '02-RECONQUISTADO'
            WHEN qtdeDiasUltTransacao <= 7 AND (qtdeDiasPenultTransacao - qtdeDiasUltTransacao) > 28 THEN '02-RENASCIDO'
        END AS descLifeCycle 
    FROM
        tb_idade AS t1
        LEFT JOIN tb_penultima_ativacao AS t2
            ON t1.IdCliente = t2.idCliente
)

-- Agora vamos conferir os totais na nossa base. A soma de todos os status, exceto o ZUMBI é o MAU
SELECT
    descLifeCycle ,
    COUNT(1)
FROM tb_life_cycle
GROUP BY descLifeCycle