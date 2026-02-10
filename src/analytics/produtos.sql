
WITH
tb_transacoes AS(
    SELECT  tr.IdTransacao
            , tr.DescSistemaOrigem
    FROM    transacoes AS tr
)

SELECT  tr.*
        , tp.idTransacaoProduto
        , tp.IdProduto
FROM    tb_transacoes AS tr
        FULL OUTER JOIN transacao_produto as tp
            ON tr.IdTransacao = tp.IdTransacao
WHERE   tr.IdTransacao IS NULL
LIMIT 100
