CREATE OR REPLACE TABLE `stackexchange_analytics.mart_developer_trends` AS
WITH unnested_tags AS (
  SELECT 
    TRIM(tag) as tag_name,
    view_count
  FROM `stackexchange_analytics.fact_hot_topics` ,
  UNNEST(tags) AS tag
  WHERE tags IS NOT NULL
)
SELECT
  tag_name,
  SUM(view_count) AS total_views,
  COUNT(*) AS total_questions,
  RANK() OVER (ORDER BY SUM(view_count) DESC) AS rank
FROM unnested_tags
WHERE tag_name != ''
GROUP BY tag_name
ORDER BY total_views DESC
LIMIT 10;