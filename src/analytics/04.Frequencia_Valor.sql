/*
Aqui vamos criar as informações de Frequencia e Valor por Usuario
*/

SELECT DISTINCT
    IdCliente , 
    -- Calcular a Frequencia : Dias distintos com interação
    COUNT(DISTINCT DATE(DtCriacao)) AS qtdeFrequencia ,
    -- Calcular o Valor : Somar pontos no periodo, que pode ser feito de 2 formas
    -- Forma 1 : desconsiderando negativos
    SUM(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosPositivos ,
    -- Forma 2 : considerando o módulo (faz mais sentido para o caso, uma vez que os negativos
    -- representam TROCA DE PONTOS, que é uma interação)
    SUM(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE ABS(QtdePontos) END) AS qtdePontosTotais ,
    -- Outra forma seria abrir essas modalidades em duas colunas representando o tipo de operação
    SUM(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS qtdePontosGanho ,
    SUM(CASE WHEN QtdePontos < 0 THEN ABS(QtdePontos) ELSE 0 END) AS qtdePontosTroca
FROM transacoes
WHERE 
    DtCriacao < '2026-02-01'
    AND DtCriacao > DATE('2026-02-01' , '-28 DAYS')
    --AND IdCliente = 'a327611d-a675-4f90-bce8-5aef803458a8'
GROUP BY IdCliente
ORDER BY DtCriacao DESC