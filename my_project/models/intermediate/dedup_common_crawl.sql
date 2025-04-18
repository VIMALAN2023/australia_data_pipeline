WITH deduplicated_common_crawl AS (
    SELECT DISTINCT ON (url)
        industry,
        title,
        url
    FROM {{ ref('stg_common_crawl') }}
    ORDER BY url
)
SELECT * FROM deduplicated_common_crawl
