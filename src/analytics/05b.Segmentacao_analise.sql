SELECT
    dtRef ,
    descLifeCycle ,
    cluster ,
    COUNT(1) as Usuarios
FROM life_cycle
WHERE descLifeCycle <> '05-ZUMBI'
GROUP BY dtRef , descLifeCycle , cluster
ORDER BY dtRef , descLifeCycle , cluster
