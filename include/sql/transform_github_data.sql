CREATE OR REPLACE TABLE `github_analytics.fact_hot_topics` AS
SELECT 
    LAX_STRING(a.raw_content.repo.name) as repo_name,
    CASE
        WHEN MAX(b.language)  IN ('Python', 'Jupyter Notebook') THEN 'Python / Data Science'
        WHEN MAX(b.language) IN ('JavaScript', 'TypeScript', 'Vue', 'React') THEN 'JavaScript Programming / Web'
        WHEN MAX(b.language) IN ('Rust', 'C++','C','Go')  THEN 'Systems Programming'
        WHEN MAX(b.language) IN ('Java','Kotlin')  THEN 'Java Programming'
        WHEN MAX(b.language) IS NULL OR MAX(b.language) = 'Unknown'  THEN 'Others / Config'
        ELSE MAX(b.language)
    END as programming_language,

    COALESCE(MAX(b.language),'Unknown') as raw_language,
    COALESCE(MAX(b.stargazers_count),0) as total_stars,
    
    COUNTIF(LAX_STRING(a.raw_content.type) = 'WatchEvent') AS new_stars_in_hour,
    COUNTIF(LAX_STRING(a.raw_content.type) = 'ForkEvent') AS new_forks_in_hour,
    COUNTIF(LAX_STRING(a.raw_content.type) = 'PushEvent') AS new_pushes_in_hour,
    (COUNTIF(LAX_STRING(a.raw_content.type) = 'WatchEvent') * 1) + (COUNTIF(LAX_STRING(a.raw_content.type) = 'ForkEvent') * 3) AS hot_score
FROM
    `github_analytics.staging_github_events` a
INNER JOIN
    `github_analytics.staging_repo_details` b
ON 
    LAX_STRING(a.raw_content.repo.name)= b.repo_name
GROUP BY
    repo_name
ORDER BY
    hot_score DESC;
    total_stars DESC;