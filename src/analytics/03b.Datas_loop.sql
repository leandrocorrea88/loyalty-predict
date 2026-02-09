-- Capturar as datas da primeira e ultima transações para loop
SELECT
    MIN(DATE(DtCriacao)) AS dtPrimeiraTransacao ,
    MAX(DATE(DtCriacao)) AS dtUltimaTransacao
FROM
    transacoes