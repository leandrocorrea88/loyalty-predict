/*
MAU (Daily Active Users), implementando a janela movel de 28 dias continuando a
granularidade de DIA na query
*/
WITH 
    -- Transações por dia e usuário no histórico
    tb_daily AS(
        SELECT DISTINCT
            DATE(DtCriacao) as DtDia ,
            IdCliente
        FROM
            transacoes
        ORDER BY DtDia
    ) ,
    -- Levantar todos o dias que houve teansação
    tb_distinct_day AS(
        SELECT
            DISTINCT DtDia AS DtRef
        FROM tb_daily
    )

-- Montar as janelas móveis
SELECT
    t1.DtRef ,
    -- Trazer a contagem dos dias apenas para conferir se a janela está sendo aplicada
    COUNT(DISTINCT t2.DtDia) AS DiasJanela ,
    COUNT(DISTINCT t2.IdCliente) AS MAU
FROM
    tb_distinct_day AS t1
    LEFT JOIN tb_daily AS t2
        -- Aplicar a janela com referência a cada dia de tb_daily
        ON t1.DtRef >= t2.DtDia
        -- A diferença é MENOR e não MENOR OU IGUAL porque na linha acima já temos uma primeira
        -- data, como se fosse um Dia=0, então o dia = 28 seria, na prática 29 dias antes
        AND JULIANDAY(t1.DtRef) - JULIANDAY(t2.DtDia) < 28
GROUP BY
    t1.DtRef
ORDER BY
    t1.DtRef DESC
--LIMIT 100