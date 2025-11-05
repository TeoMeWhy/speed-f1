WITH tb_results AS (

    SELECT 
            DriverNumber,
            DriverId,
            TeamId,
            COALESCE(int(float(Position)), 99) AS Position,
            COALESCE(int(float(GridPosition)), 99) AS GridPosition,
            Status,
            Points,
            Laps,
            identifier,
            to_date(to_timestamp(date)) AS dtEvent,
            year,
            RoundNumber,
            Location

    FROM results
    WHERE to_date(to_timestamp(date)) <= '{date}'
    
),

tb_event AS (

    SELECT DISTINCT dtEvent,
                    RoundNumber
    FROM tb_results
),

tb_drivers AS (

    SELECT DISTINCT DriverId
    FROM tb_results
    WHERE dtEvent >= (to_date('{date}') - INTERVAL 1 YEARS)

),

tb_agg_life AS (

    SELECT 
           DriverId,
           max('{date}') AS dtRef,
           year(to_date('{date}')) AS dtYear,
           count(*) AS qtdRuns,
           sum(CASE WHEN identifier = 'race' THEN 1 ELSE 0 END) AS qtdRace,
           sum(CASE WHEN identifier = 'sprint' THEN 1 ELSE 0 END) AS qtdSprint,
           
           avg(Position) AS avgPosition,
           avg(CASE WHEN identifier = 'race' THEN Position END) AS avgPositionRace,
           avg(CASE WHEN identifier = 'sprint' THEN Position END) AS avgPositionSprint,

           avg(GridPosition) AS avgGridPosition,
           avg(CASE WHEN identifier = 'race' THEN GridPosition END) AS avgGridPositionRace,
           avg(CASE WHEN identifier = 'sprint' THEN GridPosition END) AS avgGridPositionSprint,

           avg(GridPosition - Position) AS avgPositionGain,
           avg(CASE WHEN identifier = 'race' THEN GridPosition - Position END) AS avgPositionRaceGain,
           avg(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END) AS avgPositionSprintGain,

           percentile(Position, 0.5) AS medianPosition,
           percentile(CASE WHEN identifier = 'race' THEN Position END,0.5) AS medianPositionRace,
           percentile(CASE WHEN identifier = 'sprint' THEN Position END,0.5) AS medianPositionSprint,

           percentile(GridPosition, 0.5) AS medianGridPosition,
           percentile(CASE WHEN identifier = 'race' THEN GridPosition END,0.5) AS medianGridPositionRace,
           percentile(CASE WHEN identifier = 'sprint' THEN GridPosition END,0.5) AS medianGridPositionSprint,

           percentile(GridPosition - Position,0.5) AS medianPositionGain,
           percentile(CASE WHEN identifier = 'race' THEN GridPosition - Position END,0.5) AS medianPositionRaceGain,
           percentile(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END,0.5) AS medianPositionSprintGain,

           sum(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS qtdeWins,
           sum(CASE WHEN position <= 3 THEN 1 ELSE 0 END) AS qtdePodiums,
           sum(CASE WHEN GridPosition = 1 THEN 1 ELSE 0 END) AS qtdePoles

    FROM tb_results
    WHERE DriverId IN (SELECT DriverId FROM tb_drivers)
    GROUP BY DriverId

),

tb_agg_last_year AS (

    SELECT 
           DriverId,
           avg(Position) AS avgPosition1Year,
           avg(CASE WHEN identifier = 'race' THEN Position END) AS avgPositionRace1Year,
           avg(CASE WHEN identifier = 'sprint' THEN Position END) AS avgPositionSprint1Year,

           avg(GridPosition) AS avgGridPosition1Year,
           avg(CASE WHEN identifier = 'race' THEN GridPosition END) AS avgGridPositionRace1Year,
           avg(CASE WHEN identifier = 'sprint' THEN GridPosition END) AS avgGridPositionSprint1Year,

           avg(GridPosition - Position) AS avgPositionGain1Year,
           avg(CASE WHEN identifier = 'race' THEN GridPosition - Position END) AS avgPositionRaceGain1Year,
           avg(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END) AS avgPositionSprintGain1Year,

           percentile(Position, 0.5) AS medianPosition1Year,
           percentile(CASE WHEN identifier = 'race' THEN Position END,0.5) AS medianPositionRace1Year,
           percentile(CASE WHEN identifier = 'sprint' THEN Position END,0.5) AS medianPositionSprint1Year,

           percentile(GridPosition, 0.5) AS medianGridPosition1Year,
           percentile(CASE WHEN identifier = 'race' THEN GridPosition END,0.5) AS medianGridPositionRace1Year,
           percentile(CASE WHEN identifier = 'sprint' THEN GridPosition END,0.5) AS medianGridPositionSprint1Year,

           percentile(GridPosition - Position,0.5) AS medianPositionGain1Year,
           percentile(CASE WHEN identifier = 'race' THEN GridPosition - Position END,0.5) AS medianPositionRaceGain1Year,
           percentile(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END,0.5) AS medianPositionSprintGain1Year,

           sum(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS qtdeWins1Year,
           sum(CASE WHEN position <= 3 THEN 1 ELSE 0 END) AS qtdePodiums1Year,
           sum(CASE WHEN GridPosition = 1 THEN 1 ELSE 0 END) AS qtdePoles1Year

    FROM tb_results
    WHERE DriverId IN (SELECT DriverId FROM tb_drivers)
    AND dtEvent >= (to_date('{date}') - INTERVAL 1 YEARS)
    GROUP BY DriverId

),

tb_agg_current_temp AS (

    SELECT 
           DriverId,
           avg(Position) AS avgPositionCurrentTemp,
           avg(CASE WHEN identifier = 'race' THEN Position END) AS avgPositionRaceCurrentTemp,
           avg(CASE WHEN identifier = 'sprint' THEN Position END) AS avgPositionSprintCurrentTemp,

           avg(GridPosition) AS avgGridPositionCurrentTemp,
           avg(CASE WHEN identifier = 'race' THEN GridPosition END) AS avgGridPositionRaceCurrentTemp,
           avg(CASE WHEN identifier = 'sprint' THEN GridPosition END) AS avgGridPositionSprintCurrentTemp,

           avg(GridPosition - Position) AS avgPositioCurrentTemp,
           avg(CASE WHEN identifier = 'race' THEN GridPosition - Position END) AS avgPositionRaceGainCurrentTemp,
           avg(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END) AS avgPositionSprintGainCurrentTemp,

           percentile(Position, 0.5) AS medianPositionCurrentTemp,
           percentile(CASE WHEN identifier = 'race' THEN Position END,0.5) AS medianPositionRaceCurrentTemp,
           percentile(CASE WHEN identifier = 'sprint' THEN Position END,0.5) AS medianPositionSprintCurrentTemp,

           percentile(GridPosition, 0.5) AS medianGridPositionCurrentTemp,
           percentile(CASE WHEN identifier = 'race' THEN GridPosition END,0.5) AS medianGridPositionRaceCurrentTemp,
           percentile(CASE WHEN identifier = 'sprint' THEN GridPosition END,0.5) AS medianGridPositionSprintCurrentTemp,

           percentile(GridPosition - Position,0.5) AS medianPositionGainCurrentTemp,
           percentile(CASE WHEN identifier = 'race' THEN GridPosition - Position END,0.5) AS medianPositionRaceGainCurrentTemp,
           percentile(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END,0.5) AS medianPositionSprintGainCurrentTemp,

           sum(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS qtdeWinsCurrentTemp,
           sum(CASE WHEN position <= 3 THEN 1 ELSE 0 END) AS qtdePodiumsCurrentTemp,
           sum(CASE WHEN GridPosition = 1 THEN 1 ELSE 0 END) AS qtdePolesCurrentTemp,

           sum(points) AS totalPointsCurrentTemp

    FROM tb_results
    WHERE DriverId IN (SELECT DriverId FROM tb_drivers)
    AND year(dtEvent) >= year(to_date('{date}'))
    GROUP BY DriverId

)


SELECT  t4.RoundNumber AS tempRoundNumber,
        t1.*,
        t2.avgPosition1Year,
        t2.avgPositionRace1Year,
        t2.avgPositionSprint1Year,
        t2.avgGridPosition1Year,
        t2.avgGridPositionRace1Year,
        t2.avgGridPositionSprint1Year,
        t2.avgPositionGain1Year,
        t2.avgPositionRaceGain1Year,
        t2.avgPositionSprintGain1Year,
        t2.medianPosition1Year,
        t2.medianPositionRace1Year,
        t2.medianPositionSprint1Year,
        t2.medianGridPosition1Year,
        t2.medianGridPositionRace1Year,
        t2.medianGridPositionSprint1Year,
        t2.medianPositionGain1Year,
        t2.medianPositionRaceGain1Year,
        t2.medianPositionSprintGain1Year,
        t2.qtdeWins1Year,
        t2.qtdePodiums1Year,
        t2.qtdePoles1Year,
        t3.avgPositionCurrentTemp,
        t3.avgPositionRaceCurrentTemp,
        t3.avgPositionSprintCurrentTemp,
        t3.avgGridPositionCurrentTemp,
        t3.avgGridPositionRaceCurrentTemp,
        t3.avgGridPositionSprintCurrentTemp,
        t3.avgPositioCurrentTemp,
        t3.avgPositionRaceGainCurrentTemp,
        t3.avgPositionSprintGainCurrentTemp,
        t3.medianPositionCurrentTemp,
        t3.medianPositionRaceCurrentTemp,
        t3.medianPositionSprintCurrentTemp,
        t3.medianGridPositionCurrentTemp,
        t3.medianGridPositionRaceCurrentTemp,
        t3.medianGridPositionSprintCurrentTemp,
        t3.medianPositionGainCurrentTemp,
        t3.medianPositionRaceGainCurrentTemp,
        t3.medianPositionSprintGainCurrentTemp,
        t3.qtdeWinsCurrentTemp,
        t3.qtdePodiumsCurrentTemp,
        t3.qtdePolesCurrentTemp,
        t3.totalPointsCurrentTemp

FROM tb_agg_life AS t1

LEFT JOIN tb_agg_last_year AS t2
ON t1.DriverId = t2.DriverId

LEFT JOIN tb_agg_current_temp AS t3
ON t1.DriverId = t3.DriverId

LEFT JOIN tb_event AS T4
ON to_date(t1.dtRef) = to_date(t4.dtEvent)