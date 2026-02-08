/*
DAU (Daily Active Users) : Usuários ativos no dia, medido pela quantidade de pontos

Tabela de Origem: transacoes (armazenza todas as transações executadas pelos usuários)

    - IsTransacao : TEXT
    - IdCliente : TEXT
    - DtCriacao : DATETIME
    - QtdePontos : BIGINT
    - DesSistemaOrigem : TEXT
*/

SELECT
    DATE(DtCriacao) as DtDia ,
    COUNT(DISTINCT IdCliente) as DAU    -- Cada usuário pode ter mais de uma transação no dia
FROM
    transacoes
GROUP BY 1
ORDER BY DtDia