SELECT
    dtRef ,
    descLifeCycle ,
    --cluster ,
    COUNT(1) AS Usuarios
FROM life_cycle
WHERE 
    descLifeCycle <> '05-ZUMBI'
    AND dtRef = '2026-01-31'
GROUP BY dtRef , descLifeCycle -- , cluster
ORDER BY dtRef , descLifeCycle -- , cluster
