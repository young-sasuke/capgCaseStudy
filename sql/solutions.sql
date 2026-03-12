-- ============================================================
-- Car Manufacturing Case Study - SQL Analytics
-- Assumes the cleaned dataset is loaded into a table named
-- `manufacturing_events` with columns from output/cleaned_dataset.csv
-- ============================================================

-- ------------------------------------------------------------
-- 1. OEE per line
-- ------------------------------------------------------------
WITH line_stats AS (
    SELECT
        line,
        AVG(CASE WHEN cycle_time_valid THEN cycle_time_seconds END) AS actual_cycle_time_seconds,
        MAX(plan_production) AS planned_units,
        COUNT(DISTINCT vin) AS total_units,
        COUNT(DISTINCT CASE WHEN defect = 'None' AND rework = FALSE THEN vin END) AS good_units,
        SUM(CASE WHEN cycle_time_valid THEN cycle_time_seconds ELSE 0 END) AS runtime_seconds
    FROM manufacturing_events
    GROUP BY line
)
SELECT
    line,
    ROUND(LEAST(runtime_seconds / NULLIF(planned_units * 300, 0), 1.0), 4) AS availability,
    ROUND(LEAST(300 / NULLIF(actual_cycle_time_seconds, 0), 1.0), 4) AS performance,
    ROUND(good_units / NULLIF(total_units, 0), 4) AS quality,
    ROUND(
        LEAST(runtime_seconds / NULLIF(planned_units * 300, 0), 1.0)
        * LEAST(300 / NULLIF(actual_cycle_time_seconds, 0), 1.0)
        * (good_units / NULLIF(total_units, 0)),
        4
    ) AS oee
FROM line_stats
ORDER BY line;

-- ------------------------------------------------------------
-- 2. Defects per station
-- ------------------------------------------------------------
SELECT
    station,
    COUNT(*) AS defect_count
FROM manufacturing_events
WHERE defect IN ('Reject', 'Repair')
GROUP BY station
ORDER BY defect_count DESC, station;

-- ------------------------------------------------------------
-- 3. Top 5 suppliers by defect rate
-- ------------------------------------------------------------
SELECT
    supplier,
    supplier_name,
    ROUND(
        SUM(CASE WHEN defect IN ('Reject', 'Repair') THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0),
        4
    ) AS defect_rate,
    COUNT(*) AS total_events
FROM manufacturing_events
GROUP BY supplier, supplier_name
ORDER BY defect_rate DESC, total_events DESC
LIMIT 5;

-- ------------------------------------------------------------
-- 4. Bottleneck station
-- ------------------------------------------------------------
SELECT
    station,
    ROUND(AVG(cycle_time_seconds), 2) AS avg_cycle_time_seconds
FROM manufacturing_events
WHERE cycle_time_seconds IS NOT NULL
GROUP BY station
ORDER BY avg_cycle_time_seconds DESC
LIMIT 1;

-- ------------------------------------------------------------
-- 5. Average cycle time per station
-- ------------------------------------------------------------
SELECT
    station,
    ROUND(AVG(cycle_time_seconds), 2) AS avg_cycle_time_seconds
FROM manufacturing_events
WHERE cycle_time_seconds IS NOT NULL
GROUP BY station
ORDER BY station;
