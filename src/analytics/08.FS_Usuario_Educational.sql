/*
Nesse arquivo vamos montar as features do grupo Usuario - Plataforma de Cursos, dispniveis na 
documentação de features do projeto (06.Motivacao_DS.txt)

A base principal será a database.db na pasta education-platform que contem as tabelas

    Cadastros
    - cursos : Cadastro de todos os cursos disponíveis na plataforma
    - cursos_episodios : Cadastro de todos os episódios que contam em cada curso
    - habilidades : Cadastro de todas as habilidades mapeadas para um profissional de dados
    - habilidades_cargos : Mapeamento das habilidades com cargos, formando um modelo de senioridade
    a partir do nivel em cada habilidade
    - usuarios_tmw : Dicionario de Id de usuários das pessaos que estão simultaneamente na plataforma
    de cursos e acessam o TMW na Twitch

    Fatos
    - cursos_episodios_completos : Registro das datas de conclusão de cada episódio por usuário
    - recompensas_usuarios : Registro da data em que os usuários concluíram totalmente algum curso
    - habilidades_usuarios Registro onde cada usuário mapeia seu nivel em cada uma das habilidades
    disponiveis

Precisamos ter em mente que consultar diretamente as tabelas fato pode deixar algum ponto que está
cadatrado FULL de fora, então para entender o TODO usamos os Cadastros e para entender O QUE DE FATO
aconteceu olhamos as Fatos
*/

WITH

vars AS(
    SELECT
        -- Parâmetro reduz risco de SQL injection. Caso seja fornecido um parâmetro
        -- que não possa ser convertido a data se torna NULA e zera os resultados
        DATE('2025-10-01') as data_corte
) ,

-- ALIAS : cu_epi
tb_cursos_episodios AS(
    -- Contagem de episodios totais por curso disponivel
    SELECT  c.descSlugCurso
            , COUNT(c.descEpisodio) AS qtdeTotalEps
    FROM    cursos_episodios AS c
    GROUP BY c.descSlugCurso
) ,

-- ALIAS : us_rec
tb_usuario_recompensa AS (
    -- Historico de Recompensas por Usuario
    SELECT  r.idUsuario
            , r.idRecompensa
            , r.dtRecompensa
            , DATE(v.data_corte) AS dtCorte
    FROM    recompensas_usuarios AS r CROSS JOIN vars AS v
    WHERE   DATE(v.data_corte) IS NOT NULL
            AND DATE(r.dtRecompensa) < DATE(v.data_corte)
) ,

-- ALIAS : us_hab
tb_usuario_habilidade AS (
    -- Historico de cadastro de habilidades por Usuario
    SELECT  h.idUsuario
            , h.descNomeHabilidade
            , h.dtCriacao
            , DATE(v.data_corte) AS dtCorte
    FROM habilidades_usuarios AS h CROSS JOIN vars AS v
    WHERE   DATE(v.data_corte) IS NOT NULL
            AND DATE(h.dtCriacao) < DATE(v.data_corte)
) ,

-- ALIAS : us_cep
tb_usuario_cursos_episodios AS(
    -- Calcular o numero de episodios completos por usuario em cada curso, com as marcações de data
    SELECT  c.IdUsuario
            , c.descSlugCurso
            , COUNT(c.descSlugCursoEpisodio) AS qtdeEps
            -- UPDATE : Marcação de datas para feature
            , MIN(c.dtCriacao) AS dtPrimeiraDataCurso
            , MAX(c.dtCriacao) AS dtUltimaDataCurso
            , DATE(v.data_corte) AS dtCorte
    FROM    cursos_episodios_completos AS c CROSS JOIN vars AS v
    WHERE   DATE(v.data_corte) IS NOT NULL
            AND DATE(c.dtCriacao) < DATE(v.data_corte)
    GROUP BY c.idUsuario , c.descSlugCurso
) ,

-- ALIAS : us_pcu
tb_usuario_pct_cursos AS(
    -- Calcular a progressao de cada usuario nos cursos, baseado no total de episodios de cada
    -- curso cadastrado
    SELECT  us_cep.*
            , cu_epi.qtdeTotalEps
            , 1. * us_cep.qtdeEps / cu_epi.qtdeTotalEps AS pctCursoCompleto
    FROM    tb_usuario_cursos_episodios AS us_cep
            LEFT JOIN tb_cursos_episodios AS cu_epi ON us_cep.descSlugCurso = cu_epi.descSlugCurso
) ,

-- ALIAS : us_pvt
tb_usuario_pct_cursos_pivot AS(
    -- [ ] Cursos completos na plataforma (Quantidade e Quais)
    -- [ ] Cursos iniciados na plataforma (Quantidade e Quais)
    SELECT  us_pcu.IdUsuario

            -- Cursos completos na plataforma (Quantidade e Quais)
            , SUM(CASE WHEN us_pcu.pctCursoCompleto = 1 THEN 1 ELSE 0 END) AS qtdeCursosCompletos

            -- Cursos iniciados na plataforma (Quantidade e Quais)
            , SUM(CASE WHEN us_pcu.pctCursoCompleto > 0 AND us_pcu.pctCursoCompleto < 1 THEN 1 ELSE 0 END) AS qtdeCursosIncompletos

            -- Listagem de TODOS OS CURSOS disponiveis em [cursos]
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'carreira' THEN us_pcu.pctCursoCompleto END) AS Carreira
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'coleta-dados-2024' THEN us_pcu.pctCursoCompleto END) AS ColetaDados2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'data-platform-2025' THEN us_pcu.pctCursoCompleto END) AS DataPlatform2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'ds-databricks-2024' THEN us_pcu.pctCursoCompleto END) AS DsDatabricks2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'ds-pontos-2024' THEN us_pcu.pctCursoCompleto END) AS DsPontos2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'estatistica-2024' THEN us_pcu.pctCursoCompleto END) AS Estatistica2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'estatistica-2025' THEN us_pcu.pctCursoCompleto END) AS Estatistica2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'github-2024' THEN us_pcu.pctCursoCompleto END) AS GitHub2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'github-2025' THEN us_pcu.pctCursoCompleto END) AS GitHub2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'go-2026' THEN us_pcu.pctCursoCompleto END) AS Go2026
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'ia-canal-2025' THEN us_pcu.pctCursoCompleto END) AS IaCanal2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'lago-mago-2024' THEN us_pcu.pctCursoCompleto END) AS LagoMago2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'loyalty-predict-2025' THEN us_pcu.pctCursoCompleto END) AS LoyaltyPredict2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'machine-learning-2025' THEN us_pcu.pctCursoCompleto END) AS MachineLearning2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'matchmaking-trampar-de-casa-2024' THEN us_pcu.pctCursoCompleto END) AS MatchmakingTramparDeCasa2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'ml-2024' THEN us_pcu.pctCursoCompleto END) AS Ml2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'mlflow-2025' THEN us_pcu.pctCursoCompleto END) AS MlFlow2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'nekt-2025' THEN us_pcu.pctCursoCompleto END) AS Nekt2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'pandas-2024' THEN us_pcu.pctCursoCompleto END) AS Pandas2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'pandas-2025' THEN us_pcu.pctCursoCompleto END) AS Pandas2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'python-2024' THEN us_pcu.pctCursoCompleto END) AS Python2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'python-2025' THEN us_pcu.pctCursoCompleto END) AS Python2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'speed-f1' THEN us_pcu.pctCursoCompleto END) AS SpeedF1
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'sql-2020' THEN us_pcu.pctCursoCompleto END) AS SQL2020
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'sql-2025' THEN us_pcu.pctCursoCompleto END) AS SQL2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'streamlit-2025' THEN us_pcu.pctCursoCompleto END) AS Streamlit2025
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'trampar-lakehouse-2024' THEN us_pcu.pctCursoCompleto END) AS TramparLakehouse2024
            , SUM(CASE WHEN us_pcu.descSlugCurso = 'tse-analytics-2024' THEN us_pcu.pctCursoCompleto END) AS TSEAnalytics2024

    FROM    tb_usuario_pct_cursos AS us_pcu
    GROUP BY us_pcu.IdUsuario
) ,

-- ALIAS : us_atv
tb_usuario_atividade AS(
    -- Rastrear as tabelas de atividade por usuário para estimar a data mais recente de interação
    -- Fonte : Cadastro de Habilidades
    SELECT  us_hab.idUsuario
            , MAX(DATE(us_hab.dtCriacao)) AS dtUltimaInteracao
            , MAX(DATE(us_hab.dtCorte)) AS dtCorte
            , 'Cadastro Habilidade' AS descAtividade
    FROM    tb_usuario_habilidade as us_hab
    GROUP BY us_hab.idUsuario
    
    UNION ALL
    
    -- Fonte : Recompensas recebidas
    SELECT  us_rec.idUsuario
            , MAX(DATE(us_rec.dtRecompensa)) AS dtUltimaInteracao
            , MAX(DATE(us_rec.dtCorte)) AS dtCorte
            , 'Recompensa Curso' AS descAtividade
    FROM tb_usuario_recompensa AS us_rec
    GROUP BY us_rec.idUsuario
    
    UNION ALL
    
    -- Fonte : Episodios assistidos
    SELECT  us_cep.idUsuario
            , MAX(DATE(us_cep.dtUltimaDataCurso)) AS dtUltimaInteracao
            , MAX(DATE(us_cep.dtCorte)) AS dtCorte
            , 'Episodio Curso' AS descAtividade
    FROM tb_usuario_cursos_episodios AS us_cep
    GROUP BY us_cep.idUsuario
) ,

-- ALIAS : us_uat
tb_usuario_ultima_atividade AS(
    -- [ ] Dias desde a ultima interação na plataforma de cursos
    SELECT  us_atv.idUsuario
            , MIN(JULIANDAY(us_atv.dtCorte) - JULIANDAY(us_atv.dtUltimaInteracao)) AS qtdeDiasUltAtividade
    FROM tb_usuario_atividade AS us_atv CROSS JOIN vars AS v
    GROUP BY us_atv.idUsuario
)

-- Join final com o de-para de idUsuario para IdCliente
SELECT  DATE(v.data_corte , '-1 days') as dtRef
        , DATE(v.data_corte) as dtCorte
        , t.idTMWCliente as IdCliente
        , us_pvt.*
        , us_uat.qtdeDiasUltAtividade
FROM    tb_usuario_pct_cursos_pivot AS us_pvt
        CROSS JOIN vars as v
        LEFT JOIN tb_usuario_ultima_atividade AS us_uat ON us_pvt.IdUsuario = us_uat.idUsuario
        -- Usamos INNER para garantir que so teremos usuarios que estão na Twitch
        INNER JOIN usuarios_tmw as t ON us_pvt.IdUsuario = t.idUsuario

/*
A feature de habilidades ficará de fora dessa rodada, por alguns motivos
- Não é trivial de constuir
- Ainda que fosse possível, o total de usuarios ao final do INNER representa 10% dos usuarios totais
da Twitch, o que demandaria muito esforço para enriquecimento de uma parcela pequena da amostra
*/