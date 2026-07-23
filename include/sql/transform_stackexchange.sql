CREATE OR REPLACE TABLE `stackexchange_analytics.fact_hot_topics` AS
SELECT
    a.title as title,
    a.view_count AS view_count,
    a.tags AS tags,
FROM `stackexchange_analytics.staging_stackexchange_events` as a
WHERE title IS NOT NULL 
QUALIFY ROW_NUMBER() OVER(PARTITION BY title ORDER BY view_count DESC) = 1
ORDER BY 
    view_count DESC;

