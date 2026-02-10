/*
Nesse arquivo vamos montar as features do grupo Usuario - Transacional, dispniveis na documentação
de features do projeto (06.Motivacao_DS.txt)
*/

WITH

vars AS(
    SELECT
        -- Parâmetro reduz risco de SQL injection. Caso seja fornecido um parâmetro
        -- que não possa ser convertido a data se torna NULA e zera os resultados
        DATE('2025-02-01') as data_corte
) ,

-- ALIAS : tr
tb_transacao_base AS(
    -- Criar a marcação base no dia 01-02-25 como ponto de corte, 
    -- pegando a posição até o mês de 01-25 fechado
    SELECT  * 
            , DATE(t.DtCriacao) AS dtDia
            , DATE(v.data_corte) AS dtCorte
            -- UPDATE: Coluna adicionada para facilitar captura de "Periodo em que assiste a Live"
            , CAST(STRFTIME('%H', t.DtCriacao) AS INT) as dtHora
    FROM    transacoes t CROSS JOIN vars v
    WHERE   DATE(t.DtCriacao) IS NOT NULL
            AND DATE(t.DtCriacao) < v.data_corte
) ,

-- ALIAS : agg
tb_agg_transacao AS (
    -- Agregações simples da base de transações. CTE base para calculos sobre agregações
    -- Frequência em dias (D7 , D14 , D28 , D56, Vida)
    -- Frequência em transações (D7 , D14 , D28 , D56, Vida)
    -- Valor de Pontos [posit, negat, saldo] (D7 , D14 , D28 , D56, Vida)
    -- UPDATE: Periodo que assiste a live (share de periodo)
    -- UPDATE: Idade na base
    SELECT  tr.IdCliente
            -- Idade na Base -> Data da Primeira transação
            , MAX(JULIANDAY(tr.dtCorte) - JULIANDAY(tr.dtDia)) AS idadeDias
            
            -- Conntar dias distintos -> Frequencia em dias
            , COUNT(DISTINCT tr.dtDia) AS qtdeAtivacaoVida
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-7 days') THEN tr.dtDia END) AS qtdeAtivacaoD7
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-14 days') THEN tr.dtDia END) AS qtdeAtivacaoD14
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-28 days') THEN tr.dtDia END) AS qtdeAtivacaoD28
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-56 days') THEN tr.dtDia END) AS qtdeAtivacaoD56

            -- Contar transações distintas -> Frequencia em transações
            , COUNT(DISTINCT tr.IdTransacao) AS qtdeTransacaoVida
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-7 days') THEN tr.IdTransacao END) AS qtdeTransacaoD7
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-14 days') THEN tr.IdTransacao END) AS qtdeTransacaoD14
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-28 days') THEN tr.IdTransacao END) AS qtdeTransacaoD28
            , COUNT(DISTINCT CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-56 days') THEN tr.IdTransacao END) AS qtdeTransacaoD56

            -- Somar pontos positivos e negativos -> SALDO ATÉ O DIA
            , SUM(tr.QtdePontos) AS qtdePtosSaldoVida
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-7 days') THEN tr.QtdePontos ELSE 0 END) AS qtdePtosSaldoD7
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-14 days') THEN tr.QtdePontos ELSE 0 END) AS qtdePtosSaldoD14
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-28 days') THEN tr.QtdePontos ELSE 0 END) AS qtdePtosSaldoD28
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-56 days') THEN tr.QtdePontos ELSE 0 END) AS qtdePtosSaldoD56

            -- Somar pontos positivos
            , SUM(CASE WHEN tr.QtdePontos > 0 THEN tr.QtdePontos ELSE 0 END) AS qtdePtosPositVida
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-7 days') AND tr.QtdePontos > 0 THEN tr.QtdePontos ELSE 0 END) AS qtdePtosPositD7
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-28 days') AND tr.QtdePontos > 0 THEN tr.QtdePontos ELSE 0 END) AS qtdePtosPositD28
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-14 days') AND tr.QtdePontos > 0 THEN tr.QtdePontos ELSE 0 END) AS qtdePtosPositD14
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-56 days') AND tr.QtdePontos > 0 THEN tr.QtdePontos ELSE 0 END) AS qtdePtosPositD56

            -- Somar pontos negativos
            , SUM(CASE WHEN tr.QtdePontos < 0 THEN ABS(tr.QtdePontos) ELSE 0 END) AS qtdePtosNegatVida
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-7 days') AND tr.QtdePontos < 0 THEN ABS(tr.QtdePontos) ELSE 0 END) AS qtdePtosNegatD7
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-28 days') AND tr.QtdePontos < 0 THEN ABS(tr.QtdePontos) ELSE 0 END) AS qtdePtosNegatD28
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-14 days') AND tr.QtdePontos < 0 THEN ABS(tr.QtdePontos) ELSE 0 END) AS qtdePtosNegatD14
            , SUM(CASE WHEN tr.dtDia >= DATE(tr.dtCorte , '-56 days') AND tr.QtdePontos < 0 THEN ABS(tr.QtdePontos) ELSE 0 END) AS qtdePtosNegatD56

            -- Periodo em que assiste a live -> por CONTAGEM DE TRANSAÇÕES - ABSOLUTO
            -- Importante lembrar que as horas estão em UTC. Nosso fuso é UTC-3, então para adequar ao fuso vamos
            -- passar os horarios adaptadados para UTC0.
            -- Periodos (UTC-3) : 7 - 11 [Manhã] ; 12 - 18 [Tarde] ; 19 - 6 [Noite]
            -- Periodos (UTC0) : 10 - 14 [Manhã] ; 15 - 21 [Tarde] ; 21 - 10 [Noite]
            , COUNT(CASE WHEN tr.dtHora BETWEEN 10 AND 14 THEN tr.IdTransacao END) AS qtdeTransacoesManha
            , COUNT(CASE WHEN tr.dtHora BETWEEN 15 AND 21 THEN tr.IdTransacao END) AS qtdeTransacoesTarde
            , COUNT(CASE WHEN 21 < tr.dtHora OR tr.dtHora < 10 THEN tr.IdTransacao END) AS qtdeTransacoesNoite

            -- Periodo em que assiste a live -> por CONTAGEM DE TRANSAÇÕES - RELATIVO AO TOTAL DE TRANSAÇÕES
            -- Para o modelo é melhor pegar a versão
            , 1. * COUNT(CASE WHEN tr.dtHora BETWEEN 10 AND 14 THEN tr.IdTransacao END) /  NULLIF(COUNT(DISTINCT tr.IdTransacao), 0) AS pctTransacoesManha
            , 1. * COUNT(CASE WHEN tr.dtHora BETWEEN 15 AND 21 THEN tr.IdTransacao END) /  NULLIF(COUNT(DISTINCT tr.IdTransacao), 0) AS pctTransacoesTarde
            , 1. * COUNT(CASE WHEN 21 < tr.dtHora OR tr.dtHora < 10 THEN tr.IdTransacao END) /  NULLIF(COUNT(DISTINCT tr.IdTransacao), 0) AS pctTransacoesNoite

    FROM tb_transacao_base AS tr
    GROUP BY tr.IdCliente
) ,

-- ALIAS: agg_calc
tb_agg_calculado AS (
    -- Calculos sobre agregações das transações
    -- Quantidade de transações por dia (D7 , D14 , D28 , D56)
    -- Percentual de ativação Do MAU
    SELECT  agg.*
            -- Multiplicar por "1." para tranformar em FLOAT e encapsular em COALESCE para tratar os NULOS como ZERO
            -- Como COLAESCE não trata divisão por zero vamos tratar manualmente com NULLIF
            , COALESCE(1. * agg.qtdeTransacaoVida / NULLIF(agg.qtdeAtivacaoVida, 0) , 0 ) AS qtdeTransacaoDiaVida
            , COALESCE(1. * agg.qtdeTransacaoD7 / NULLIF(agg.qtdeAtivacaoD7, 0) , 0 ) AS qtdeTransacaoDiaD7
            , COALESCE(1. * agg.qtdeTransacaoD14 / NULLIF(agg.qtdeAtivacaoD14, 0) , 0 ) AS qtdeTransacaoDiaD14
            , COALESCE(1. * agg.qtdeTransacaoD28 / NULLIF(agg.qtdeAtivacaoD28, 0) , 0 ) AS qtdeTransacaoDiaD28
            , COALESCE(1. * agg.qtdeTransacaoD56 / NULLIF(agg.qtdeAtivacaoD56, 0) , 0 ) AS qtdeTransacaoDiaD56

            -- Ativacao do MAU : qtdeTransacoesD28 na janela do MAU
            , COALESCE(1. * qtdeAtivacaoD28 / 28 , 0) AS pctAtivacaoMAU

    FROM    tb_agg_transacao AS agg
) ,

-- ALIAS hr_d
tb_horas_dia AS(
    -- Horas assistidas (D7 , D14 , D28 , D56) : PARTE 1
    SELECT  tr.IdCliente
            , tr.dtDia
            -- Capturar a primeira e ultima transações no dia. 
            -- PREMISSAS: 
            -- a. Pessoas que somente assistiram e não interagiram não irão aparecer nessa base
            -- b. Pessoas com somente UMA transação (!presente) terão valor = 0
            -- , MIN(JULIANDAY(tr.DtCriacao)) AS dtPrimeiraTransacaoLive
            -- , MAX(JULIANDAY(tr.DtCriacao)) AS dtUltimaTransacaoLive
            -- Calcular a diferença em HORAS (x24). Se fosse em minutos multiplicariamos por 60
            , 24 * (MAX(JULIANDAY(tr.DtCriacao)) - MIN(JULIANDAY(tr.DtCriacao))) AS duracao
            , tr.dtCorte
    FROM    tb_transacao_base AS tr
    GROUP BY    tr.IdCliente , tr.dtDia
) ,

-- ALIAS : hr_u
tb_horas_usuario AS(
    -- Horas assistidas (D7 , D14 , D28 , D56) : PARTE 2
    SELECT  hr_d.IdCliente
            , SUM(hr_d.duracao) AS qtdeHorasVida
            , SUM(CASE WHEN hr_d.dtDia >= DATE(hr_d.dtCorte , '-7 days') THEN hr_d.duracao ELSE 0 END ) AS qtdeHorasD7
            , SUM(CASE WHEN hr_d.dtDia >= DATE(hr_d.dtCorte , '-14 days') THEN hr_d.duracao ELSE 0 END ) AS qtdeHorasD14
            , SUM(CASE WHEN hr_d.dtDia >= DATE(hr_d.dtCorte , '-28 days') THEN hr_d.duracao ELSE 0 END ) AS qtdeHorasD28
            , SUM(CASE WHEN hr_d.dtDia >= DATE(hr_d.dtCorte , '-56 days') THEN hr_d.duracao ELSE 0 END ) AS qtdeHorasD56
    FROM    tb_horas_dia AS hr_d
    GROUP BY hr_d.IdCliente
) ,

--ALIAS : lag_d
tb_lag_dia AS(
    -- Media de intervalo entre os dias que o usuário aparece : PARTE 1
    -- Vamos usar a horas_dia porque ela JA FEZ O FILTRO DOS DIAS QUE O USUÁRIO APARECEU
    -- PREMISSA: Consideramos o dia quando já transação
    SELECT  hr_d.IdCliente
            , hr_d.dtDia
            , LAG(hr_d.dtDia, 1) OVER (PARTITION BY hr_d.IdCliente ORDER BY hr_d.dtDia ASC) AS lagDia
            , hr_d.dtCorte
    FROM    tb_horas_dia AS hr_d
) ,

-- ALIAS : int_d
tb_intervalo_dias AS(
    -- Media de intervalo entre os dias que o usuário aparece : PARTE 2
    SELECT  lag_d.IdCliente
            -- Diferença de dias linha a linha
            --, JULIANDAY(lag_d.dtDia) - JULIANDAY(lag_d.lagDia) AS diifDia
            -- Médias das diferenças VIDA
            , AVG(JULIANDAY(lag_d.dtDia) - JULIANDAY(lag_d.lagDia)) AS avgIntervaloDiasVida
            -- Media das diferenças por data de corte (puxando das CTEs anteriores) - USAR SOMENTE MAU
            --, AVG(CASE WHEN lag_d.dtDia >= DATE(lag_d.dtCorte , '-7 days') THEN JULIANDAY(lag_d.dtDia) - JULIANDAY(lag_d.lagDia) END) AS avgIntervaloDiasD7
            --, AVG(CASE WHEN lag_d.dtDia >= DATE(lag_d.dtCorte , '-14 days') THEN JULIANDAY(lag_d.dtDia) - JULIANDAY(lag_d.lagDia) END) AS avgIntervaloDiasD14
            , AVG(CASE WHEN lag_d.dtDia >= DATE(lag_d.dtCorte , '-28 days') THEN JULIANDAY(lag_d.dtDia) - JULIANDAY(lag_d.lagDia) END) AS avgIntervaloDiasD28
            --, AVG(CASE WHEN lag_d.dtDia >= DATE(lag_d.dtCorte , '-56 days') THEN JULIANDAY(lag_d.dtDia) - JULIANDAY(lag_d.lagDia) END) AS avgIntervaloDiasD56
    FROM    tb_lag_dia as lag_d
    GROUP BY lag_d.IdCliente
) ,

-- ALIAS : shr_p
tb_share_produtos AS(
        /*
        A categorização de produtos será feita de maneira mista, uma vez que temos uma estrutura com 35 
        produtos distribuídos em 7 categorias. Porém algumas categorias tem poucos elementos. Então para
        esse caso vamos trabalhar as categorias 'rpg' e 'churn_model' como AGRUPADAS enquanto que os demais
        produtos entrarão de maneira explicita. Também já vamos fazer TRANSPOSTO para garantir que cada
        cliente permaneça com apenas 1 LINHA
        
        E da mesma forma que fizemos com Share de Manha vs Tarde vs Noite, aqui tambem vamos capturar apenas
        as proporções de transações em cada um dos estratos
        
        IMPORTANTE: Há cerca de 19 produtos que tem seu codigo em transação, mas NÃO TEM CADASTRO. Portanto
        devemos ter em mente que nem todos os produtos estarão mapeados
        
        */
        -- Tipos de produtos consumidos (essa abertura permite posterior ANALISE DE CLUSTERING DE USUÁRIOS)
        SELECT  tr.IdCliente
                -- Agrupando as CATEGORIAS
                , 1. * COUNT(CASE WHEN t3.DescCategoriaProduto = 'rpg' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeRPG
                , 1. * COUNT(CASE WHEN t3.DescCategoriaProduto = 'churn_model' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeChurnModel
                -- Transpondo os PRODUTOS não agrupados
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'ChatMessage' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeChatMessage
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'Airflow Lover' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeAirflowLover
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'R Lover' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeRLover
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'Resgatar Ponei' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeResgatarPonei
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'Lista de presença' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeListaDePresença
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'Presença Streak' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdePresençaStreak
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'Troca de Pontos StreamElements' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeTrocaDePontos
                , 1. * COUNT(CASE WHEN t3.DescNomeProduto = 'Reembolso: Troca de Pontos StreamElements' THEN tr.IdTransacao END) / COUNT(DISTINCT tr.IdTransacao) AS qtdeReembolsoDePontos
        FROM    tb_transacao_base AS tr
                -- Join com a tabela de transacao por produto
                LEFT JOIN transacao_produto AS t2
                ON tr.IdTransacao = t2.IdTransacao
                -- Join com a tabela de produtos
                LEFT JOIN produtos AS t3
                ON t2.IdProduto = t3.IdProduto
        GROUP BY tr.IdCliente
) ,

-- ALIAS : join
tb_join AS(
    -- Consolidar as features (Até agora)
    SELECT  DATE(v.data_corte , '-1 days') AS dtRef
            , v.data_corte AS dtCorte
            
            , agg_calc.*
            
            , hr_u.qtdeHorasVida
            , hr_u.qtdeHorasD7
            , hr_u.qtdeHorasD14
            , hr_u.qtdeHorasD28
            , hr_u.qtdeHorasD56
            
            , int_d.avgIntervaloDiasVida
            , int_d.avgIntervaloDiasD28
            
            , shr_p.qtdeRPG
            , shr_p.qtdeChurnModel
            , shr_p.qtdeChatMessage
            , shr_p.qtdeAirflowLover
            , shr_p.qtdeRLover
            , shr_p.qtdeResgatarPonei
            , shr_p.qtdeListaDePresença
            , shr_p.qtdePresençaStreak
            , shr_p.qtdeTrocaDePontos
            , shr_p.qtdeReembolsoDePontos

    FROM    tb_agg_calculado AS agg_calc
            CROSS JOIN vars as v
            LEFT JOIN tb_horas_usuario AS hr_u ON agg_calc.IdCliente = hr_u.IdCliente
            LEFT JOIN tb_intervalo_dias AS int_d ON agg_calc.IdCliente = int_d.IdCliente
            LEFT JOIN tb_share_produtos AS shr_p ON agg_calc.IdCliente = shr_p.IdCliente
)

SELECT *
FROM tb_join