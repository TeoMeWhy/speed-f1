WITH points_acum AS (

    SELECT  year,
            DriverId,
            sum(Points) AS total_points
    
    FROM results
    
    GROUP BY 1,2
    ORDER BY 1,3 DESC

),

tb_rn AS (
    
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_points desc) AS rn_driver
    
    FROM points_acum
)

SELECT *
FROM tb_rn
WHERE rn_driver = 1