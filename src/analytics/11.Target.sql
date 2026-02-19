/*
Criar a variavel target.

CONCEITOS IMPORTANTES: Maturação de Safra/Base e Amostragem Independente

1. Maturação da Safra: Precisamos saber qual é o tempo que o fenômeno que queremos analisar leva 
para ser observado. No nosso caso concreto, que é prever a probabilidade de um usuário qualquer 
estar em FIEL nos próximos 28 dias já nos dá esse parâmetro. Outros casos podem fazer esse numero 
aumentar ou diminuir, por exemplo, para fraudes o tempo pode ser de até 6 meses.

2. Independência da amostra: aqui estamos lidando com dados temporais, então a peimeira pergunnta a
fazer seria "Não poderia pegar toda a minha base historica, que está em dias, e usar para treinar o
meu modelo?". O grande ponto aqui é que estamos prevendo COMPORTAMENTO, e hábitos não são criados de
um dia para o outro. No caso dos usuários, olhar em D e D-1 temos praticamente "a mesma pessoa", e
usar essa base para o modelo pode provocar um VIES (ou ENVIESAR o modelo), uma vez que alguem que é
FIEL recorrentemente em base DIARIA tende a permanecer como tal. Então para isso podemos ter algumas
alternativas para reduzir esse viés

        a) Aumentar o intervalo de coleta: ao comparar de um dia para o outro a pessoa é praticamente
        a mesma, porém ao capturar fotos MENSAIS, existe a chance maior de absorver algumas mudanças

        b) Aleatorizar as amostras capturadas das pessoas

O que sabemos sobre os nossos dados?

        a. Cada usuário vai aparecer MUITAS vezes na base
        
        b. Idealmente, deveriamos pegar 1 amostra de cada usuários, mas como temos poucos dados
        vamos capturar 2 amostrar de cada usuário
        
        c. O custo para contatar um cliente pode ser alto, e a porcentagem de ZUMBI que voltam a
        interagir é muito baixa então vamos retira-los da nossa amostra

*/

-- Cruzamos cada cliente consigo mesmo dentro do intervalo esperado na janela de maturação.

DROP TABLE IF EXISTS abt_flFiel;

CREATE TABLE abt_flFiel AS

WITH 

tb_exemplo_completo AS(
-- Essa tabela tem valor pedagógico, para mostrar que precisamos apenas do usuario e data de
-- referência com o estado futuro que queremos prever, no caso, ser FIEL
        SELECT  t1.dtRef AS t1_dtRef
                , t1.IdCliente AS t1_IdCliente
                , t1.descLifeCycle AS t1_descLifeCycle
                , t2.dtRef AS t2_dtRef
                , t2.descLifeCycle AS t2_descLifeCycle
        FROM    life_cycle AS t1
                LEFT JOIN life_cycle AS t2 
                ON t1.IdCliente = t2.IdCliente
                AND DATE(t1.dtRef , '+28 days') = DATE(t2.dtRef)
        WHERE t1.dtRef = DATE('2025-09-01')
                -- Se quisessemos aumentar o intervalo de tempo para pegar apenas o dia 1 de cada mes
                AND SUBSTR(t1.dtRef , 8 , 2) = '01'
                -- Se quisessemos remover os zumbis da base
                AND t1.descLifeCycle = "05-ZUMBI"
        LIMIT 100
) ,

tb_target_abt AS(
        -- Nessa versão da tabela vamos manter apenas os campos que vamos precisar para criar
        -- noosa variável target
        SELECT  t1.dtRef 
                , t1.IdCliente
                -- Vamos manter os 2 life cycle apenas para comparação
                , t1.descLifeCycle AS t1_descLifeCycle
                , t2.descLifeCycle AS t2_descLifeCycle
                -- Variavel target depende apenas do segundo que é: QUAL A PROBABILIDADE
                -- DE UM USUÁRIO SER FIEL NO PROXIMO MAU?
                , CASE WHEN t2.descLifeCycle = '02-FIEL' THEN 1 ELSE 0 END AS flFiel
                -- Numerar aleatoriamente os lançamentos dos usuários
                , ROW_NUMBER() OVER (   PARTITION BY t1.IdCliente 
                                        ORDER BY RANDOM()) AS RandomRow
        
        FROM    life_cycle AS t1
                LEFT JOIN life_cycle AS t2 
                ON t1.IdCliente = t2.IdCliente
                -- Join na janela de maturação
                AND DATE(t1.dtRef , '+28 days') = DATE(t2.dtRef)
        
        WHERE   t1.descLifeCycle <> '05-ZUMBI'
                AND DATE(t1.dtRef) >= DATE('2025-03-01')
                -- Excluir estratos OUT OF TIME
                AND DATE(t1.dtRef) < DATE('2026-01-01')
) , 

tb_cohort_abt AS(
        -- ABT para modelagem (Treino + Teste)
        SELECT  t1.dtRef
                , t1.IdCliente
                , t1.flFiel
                ,'ABT' as tpDado
        FROM tb_target_abt AS t1
        WHERE t1.RandomRow <= 2         -- 2 amostras de cada cliente
        ORDER BY t1.IdCliente , t1.dtRef
) ,

tb_target_oot AS(
        -- Lotes Out Of Time para treinamento posterior do modelo validado no SEMMA
        SELECT  t1.dtRef 
                , t1.IdCliente
                -- Vamos manter os 2 life cycle apenas para comparação
                , t1.descLifeCycle AS t1_descLifeCycle
                , t2.descLifeCycle AS t2_descLifeCycle
                , CASE WHEN t2.descLifeCycle = '02-FIEL' THEN 1 ELSE 0 END AS flFiel
                -- Numerar aleatoriamente os lançamentos dos usuários
                , ROW_NUMBER() OVER (   PARTITION BY t1.IdCliente 
                                        ORDER BY RANDOM()) AS RandomRow
       
        FROM    life_cycle AS t1
                LEFT JOIN life_cycle AS t2 
                ON t1.IdCliente = t2.IdCliente
                AND DATE(t1.dtRef , '+28 days') = DATE(t2.dtRef)
       
        WHERE   t1.descLifeCycle <> '05-ZUMBI'
                AND DATE(t1.dtRef) >= DATE('2025-03-01')
                -- 3 Lotes OUT OF TIME para treinar o modelo
                AND DATE(t1.dtRef) BETWEEN DATE('2026-01-01') AND DATE('2026-01-03')
) ,

tb_cohort_oot AS(
        -- ABT Out Of Time para validação pós SEMMA
        SELECT  t1.dtRef
                , t1.IdCliente
                , t1.flFiel
                ,'OOT' as tpDado
        FROM tb_target_oot AS t1
        WHERE t1.RandomRow <= 2         -- 2 amostras de cada cliente
        ORDER BY t1.IdCliente , t1.dtRef
) ,

tb_join AS(
        SELECT * FROM tb_cohort_abt
        UNION ALL
        SELECT * FROM tb_cohort_oot
)

SELECT  t1.*

        , tr.idadeDias
        , tr.qtdeAtivacaoVida
        , tr.qtdeAtivacaoD7
        , tr.qtdeAtivacaoD14
        , tr.qtdeAtivacaoD28
        , tr.qtdeAtivacaoD56
        , tr.qtdeTransacaoVida
        , tr.qtdeTransacaoD7
        , tr.qtdeTransacaoD14
        , tr.qtdeTransacaoD28
        , tr.qtdeTransacaoD56
        , tr.qtdePtosSaldoVida
        , tr.qtdePtosSaldoD7
        , tr.qtdePtosSaldoD14
        , tr.qtdePtosSaldoD28
        , tr.qtdePtosSaldoD56
        , tr.qtdePtosPositVida
        , tr.qtdePtosPositD7
        , tr.qtdePtosPositD28
        , tr.qtdePtosPositD14
        , tr.qtdePtosPositD56
        , tr.qtdePtosNegatVida
        , tr.qtdePtosNegatD7
        , tr.qtdePtosNegatD28
        , tr.qtdePtosNegatD14
        , tr.qtdePtosNegatD56
        , tr.qtdeTransacoesManha
        , tr.qtdeTransacoesTarde
        , tr.qtdeTransacoesNoite
        , tr.pctTransacoesManha
        , tr.pctTransacoesTarde
        , tr.pctTransacoesNoite
        , tr.qtdeTransacaoDiaVida
        , tr.qtdeTransacaoDiaD7
        , tr.qtdeTransacaoDiaD14
        , tr.qtdeTransacaoDiaD28
        , tr.qtdeTransacaoDiaD56
        , tr.pctAtivacaoMAU
        , tr.qtdeHorasVida
        , tr.qtdeHorasD7
        , tr.qtdeHorasD14
        , tr.qtdeHorasD28
        , tr.qtdeHorasD56
        , tr.avgIntervaloDiasVida
        , tr.avgIntervaloDiasD28
        , tr.qtdeRPG
        , tr.qtdeChurnModel
        , tr.qtdeChatMessage
        , tr.qtdeAirflowLover
        , tr.qtdeRLover
        , tr.qtdeResgatarPonei
        , tr.qtdeListaDePresença
        , tr.qtdePresençaStreak
        , tr.qtdeTrocaDePontos
        , tr.qtdeReembolsoDePontos

        , ed.qtdeCursosCompletos
        , ed.qtdeCursosIncompletos
        , ed.Carreira
        , ed.ColetaDados2024
        , ed.DataPlatform2025
        , ed.DsDatabricks2024
        , ed.DsPontos2024
        , ed.Estatistica2024
        , ed.Estatistica2025
        , ed.GitHub2024
        , ed.GitHub2025
        , ed.Go2026
        , ed.IaCanal2025
        , ed.LagoMago2024
        , ed.LoyaltyPredict2025
        , ed.MachineLearning2025
        , ed.MatchmakingTramparDeCasa2024
        , ed.Ml2024
        , ed.MlFlow2025
        , ed.Nekt2025
        , ed.Pandas2024
        , ed.Pandas2025
        , ed.Python2024
        , ed.Python2025
        , ed.SpeedF1
        , ed.SQL2020
        , ed.SQL2025
        , ed.Streamlit2025
        , ed.TramparLakehouse2024
        , ed.TSEAnalytics2024
        , ed.qtdeDiasUltAtividade

        , lc.descLifeCycleAtual
        , lc.qtdeFrequencia
        , lc.descLifeCycleD28
        , lc.pct01_CURIOSO
        , lc.pct02_FIEL
        , lc.pct02_RECONQUISTADO
        , lc.pct02_RENASCIDO
        , lc.pct03_TURISTA
        , lc.pct04_DESENCANTADO
        , lc.pct05_ZUMBI
        , lc.avgFreqGrupo
        , lc.ratioFreqGrupo
        , lc.avgFreqD7
        , lc.avgFreqD14
        , lc.ratioFreqUsuarioD7
        , lc.ratioFreqUsuarioD14

FROM    tb_join AS t1
        LEFT JOIN fs_transacional AS tr
                ON      t1.IdCliente = tr.IdCliente
                AND     t1.dtRef = tr.dtRef
        LEFT JOIN fs_educational AS ed
                ON      t1.IdCliente = ed.IdCliente
                AND     t1.dtRef = ed.dtRef
        LEFT JOIN fs_life_cycle AS lc
                ON      t1.IdCliente = lc.IdCliente
                AND     t1.dtRef = lc.dtRef