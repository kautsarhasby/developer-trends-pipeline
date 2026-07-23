CREATE OR REPLACE TABLE `github_analytics.mart_developer_trends` AS 
SELECT
    programming_language,
    SUM(hot_score) AS total_hot_score,
    RANK() OVER (ORDER BY SUM(hot_score) DESC) AS rank_language
FROM `github_analytics.fact_hot_topics`
WHERE programming_language != 'Unknown'
GROUP BY programming_language
ORDER BY rank_language ASC
LIMIT 10;
