/*
Aqui vamos ROTULAR os clusters de usuarios
*/

WITH
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
        DtCriacao < '2026-02-01'
        AND DtCriacao > DATE('2026-02-01' , '-28 DAYS')
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

SELECT
    cluster ,
    COUNT(1)
FROM tb_cluster
GROUP BY 1
ORDER BY 1 DESC