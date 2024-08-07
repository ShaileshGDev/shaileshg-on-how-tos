
-- Design tests for 
-- Percent of Type population on current version of the table

-- Percent of Type population on previous version of the table for last three to five days

-- Flag the type of devices where there is a change of percent population lets say 5% or 2%

-- databricks sql query to see Percent of Type population on previous days   last three 7 days

-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW population AS
SELECT * FROM VALUES
('2024-08-01', 'c1', 10000),
('2024-08-01', 'c2', 10000),
('2024-08-01', 'c3', 10000),
('2024-08-02', 'c1', 10099),
('2024-08-02', 'c2', 10900),
('2024-08-02', 'c3', 19000),
('2024-08-03', 'c1', 19000),
('2024-08-03', 'c2', 10700),
('2024-08-03', 'c3', 10030),
('2024-08-04', 'c1', 7000),
('2024-08-04', 'c2', 10600),
('2024-08-04', 'c3', 10010),
('2024-08-05', 'c1', 11000),
('2024-08-05', 'c2', 10030),
('2024-08-05', 'c3', 17000),
('2024-08-07', 'c1', 18000),
('2024-08-07', 'c2', 10080),
('2024-08-07', 'c3', 15000),
('2024-08-08', 'c1', 10300),
('2024-08-08', 'c2', 10000),
('2024-08-08', 'c3', 15000)
AS population(date, type, count);

-- COMMAND ----------

-- MAGIC %sql select * from population

-- COMMAND ----------

-- DBTITLE 1,Percent of Type population on current version of the table
--1.calculate in sum count of recent date as total count 
--2.divide :count / sum count *100
--3.all this from population filtering date 


-- COMMAND ----------

select date,type,count,
  SUM(count) OVER (PARTITION BY date) AS total_count,
   ((count / SUM(count) OVER (PARTITION BY date)) * 100) AS percent_population 
FROM
   population
where Date = '2024-08-08'
ORDER BY
   date, type;

-- COMMAND ----------

SELECT
       date,
       type,
       count,
       SUM(count) OVER (PARTITION BY date) AS total_population,
       round(count * 100.0 / SUM(count) OVER (PARTITION BY date), 2) AS percent_of_total_type

   FROM
       population


-- COMMAND ----------

--by using Percentage Change=( Privious Count−Current Count / previous count )×100


-- COMMAND ----------

-- DBTITLE 1,2-Percent of Type pop on previous version of the table for last 3to 5days
WITH percentage_count AS (
    SELECT
        date,
        type,
        count,
        AVG(count) OVER (PARTITION BY type ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS previous_cnt
    FROM
        population
),
PercentageChange AS (
    SELECT
        date,
        type,
        count,
        previous_cnt,
        ((count - previous_cnt) / previous_cnt) * 100 AS percent_change
    FROM percentage_count
        )
SELECT *
FROM PercentageChange
WHERE 
    date BETWEEN '2024-08-04' AND '2024-08-08'
ORDER BY
    date, type;

-- COMMAND ----------

-- DBTITLE 1,3-Flag the type of devices where there is a change of percent population lets say 5% or 2%
WITH MovingAverage AS (
    SELECT
        date,
        type,
        count,
        AVG(count) OVER (PARTITION BY type ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
    FROM
        population
),
PercentageChange AS (
    SELECT
        date,
        type,
        count,
        moving_avg,
        round(((count - moving_avg) / moving_avg) * 100) AS percent_change
    FROM
        MovingAverage
),
Flagged AS (
    SELECT
        date,
        type,
        count,
        moving_avg,
        percent_change,
        CASE
            WHEN ABS(percent_change) >= 5 THEN 'Flagged (5%)'
            WHEN ABS(percent_change) >= 2 THEN 'Flagged (2%)'
            ELSE 'Not Flagged'
        END AS flag
    FROM
        PercentageChange
)
SELECT
    *
FROM
    Flagged
ORDER BY
    date, type;


