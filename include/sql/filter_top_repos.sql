CREATE OR REPLACE TABLE `github_analytics.staging_top_repos` AS 
SELECT
    LAX_STRING(raw_content.repo.name) as repo_name,
    LAX_STRING(raw_content.repo.url) as repo_url,
    COUNTIF(LAX_STRING(raw_content.type) = 'WatchEvent') AS total_stars,
    COUNTIF(LAX_STRING(raw_content.type) = 'ForkEvent') AS total_forks,
    COUNTIF(LAX_STRING(raw_content.type) = 'PushEvent') AS total_pushes,
    (COUNTIF(LAX_STRING(raw_content.type) = 'WatchEvent') * 3) +
    (COUNTIF(LAX_STRING(raw_content.type) = 'ForkEvent') * 5) + LEAST(COUNTIF(LAX_STRING(raw_content.type) = 'PushEvent'), 50) AS activity_score
FROM
    `github_analytics.staging_github_events`
WHERE
    LAX_STRING(raw_content.type) in ('WatchEvent', 'PushEvent','ForkEvent')
    AND LAX_STRING(raw_content.repo.name) NOT LIKE '%/ptlveq%'
GROUP BY
    repo_name,
    repo_url
HAVING
    (COUNTIF(LAX_STRING(raw_content.type) = 'ForkEvent') + COUNTIF(LAX_STRING(raw_content.type) = 'WatchEvent')) > 0
ORDER BY
    activity_score DESC
LIMIT 20