/*
Nesse arquivo vamos montar as features do grupo Usuario - Ciclo de Vida, dispnoiveis na 
documentação de features do projeto (06.Motivacao_DS.txt)

Fontes de Dados:

        - Base de dados ANALITICA de loyalty-system

*/

WITH

vars AS (
        -- A base analítica está montada com base no ultimo dia de cada mês, mas o parâmetro tende
        -- a ser o primeiro dia do mes subsequente
        SELECT 
                -- Após as validações mudamos de hardcode para parametrizado
                DATE('{_date}') as data_corte
                --DATE('2026-02-01') AS data_corte
                
) ,

-- ALIAS : lc_at
tb_life_cycle_atual AS(
        -- [ ] Ciclo de vida atual
        SELECT  lc.IdCliente 
                , lc.descLifeCycle AS descLifeCycleAtual
                -- UPDATE: para comparar a frequencia do usuario com cluster vamos trazer a 
                -- frequencia para a consulta e retirar do join
                , lc.qtdeFrequencia
        FROM    life_cycle AS lc
                CROSS JOIN vars AS v

        WHERE   DATE(v.data_corte) IS NOT NULL
                AND lc.dtRef = DATE(v.data_corte)
) ,

-- ALIAS lc_d28
tb_life_cycle_D28 AS(
        -- [ ] Ciclo de vida no MAU de D-28
        -- A base analítica foi tirada em datas específicas do ultimo dia de cada mes. Então vamos 
        -- precisar voltar ao python para gerar novas datas
        -- Após a geração da data podemos executar novamete a consulta
        SELECT  lc.IdCliente 
                , lc.descLifeCycle AS descLifeCycleD28

        FROM    life_cycle AS lc
                CROSS JOIN vars AS v

        WHERE   DATE(v.data_corte) IS NOT NULL
                AND lc.dtRef = DATE(v.data_corte , '-29 days')
) ,

-- ALIAS : lc_shr
tb_share_ciclos AS(
        -- [ ] Já foi zumbi? (flag)
        -- [ ] Quantidade de dias em cada status do ciclo de vida
        -- Aqui calculamos a % de dias em que cada usuário permaneceu em cada classificação de lifecyle
        SELECT   lc.IdCliente
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '01-CURIOSO' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct01_CURIOSO
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '02-FIEL' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct02_FIEL
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '02-RECONQUISTADO' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct02_RECONQUISTADO
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '02-RENASCIDO' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct02_RENASCIDO
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '03-TURISTA' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct03_TURISTA
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '04-DESENCANTADO' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct04_DESENCANTADO
                ,1. *  SUM(CASE WHEN lc.descLifeCycle = '05-ZUMBI' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct05_ZUMBI
        FROM    life_cycle AS lc
                CROSS JOIN vars as v
        WHERE   DATE(v.data_corte) IS NOT NULL
                AND lc.dtRef < DATE(v.data_corte)
        GROUP BY lc.IdCliente
) ,

-- ALIAS : gp_avg
tb_avg_ciclo AS(
        -- Calcular as médias de frequencia de cada grupo para agregar ao join e calcular o
        -- lift do usuario
        SELECT  lc_act.DescLifeCycleAtual as DescLifeCycle
                , AVG(qtdeFrequencia) as avgFreqGrupo
        FROM tb_life_cycle_atual AS lc_act
        GROUP BY lc_act.DescLifeCycleAtual
) ,

-- ALIAS : us_hist
tb_freq_moving_usuario AS(
-- Calcular as frequencias de cada usuario em janelas de até 14 dias para agregar ao join 
-- e calcular o lift do usuario sobre ele mesmo
        SELECT  lc.IdCliente
                , lc.dtRef
                , lc.qtdeFrequencia
                , ROW_NUMBER() OVER (PARTITION BY lc.IdCliente ORDER BY lc.dtRef DESC) as rn
        FROM    life_cycle AS lc
                CROSS JOIN vars as v
        WHERE   lc.dtRef BETWEEN DATE(v.data_corte, '-13 DAYS') AND DATE(v.data_corte)
) ,

-- ALIAS : us_avg
tb_avg_usuario AS(
-- Calcular as médias de frequencia por usuário nas janelas de D7 e D14
        SELECT  us_hist.IdCliente
                , AVG(CASE WHEN rn <= 7 THEN us_hist.qtdeFrequencia END) AS avgFreqD7
                , AVG(us_hist.qtdeFrequencia) AS avgFreqD14
        FROM    tb_freq_moving_usuario AS us_hist
        GROUP BY us_hist.IdCliente
) ,

-- ALIAS : lc_join
tb_life_cycle_join AS(
        -- [ ] Calcular a média de frequencia de cada grupo de Life Cycle no periodo
        SELECT  lc_at.*
                
                , lc_d28.descLifeCycleD28

                , lc_shr.pct01_CURIOSO
                , lc_shr.pct02_FIEL
                , lc_shr.pct02_RECONQUISTADO
                , lc_shr.pct02_RENASCIDO
                , lc_shr.pct03_TURISTA
                , lc_shr.pct04_DESENCANTADO
                , lc_shr.pct05_ZUMBI

                , gp_avg.avgFreqGrupo
                , lc_at.qtdeFrequencia / gp_avg.avgFreqGrupo AS ratioFreqGrupo

                , us_avg.avgFreqD7
                , us_avg.avgFreqD14
                , lc_at.qtdeFrequencia / us_avg.avgFreqD7 AS ratioFreqUsuarioD7
                , lc_at.qtdeFrequencia / us_avg.avgFreqD14 AS ratioFreqUsuarioD14

        FROM    tb_life_cycle_atual AS lc_at
                LEFT JOIN tb_life_cycle_D28 AS lc_d28 ON lc_at.IdCliente = lc_d28.IdCliente
                LEFT JOIN tb_share_ciclos AS lc_shr ON lc_at.IdCliente = lc_shr.IdCliente
                LEFT JOIN tb_avg_ciclo AS gp_avg ON lc_at.descLifeCycleAtual = gp_avg.DescLifeCycle
                LEFT JOIN tb_avg_usuario AS us_avg ON lc_at.IdCliente = us_avg.IdCliente
)

SELECT  DATE(v.data_corte , '-1 days') AS dtRef
        , v.data_corte AS dtCorte
        , lc_join.* 
FROM    tb_life_cycle_join AS lc_join
        CROSS JOIN vars AS v