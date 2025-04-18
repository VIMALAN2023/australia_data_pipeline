-- Clean and normalize common_crawl_data
WITH clean_common_crawl AS (
    SELECT
        lower(trim(industry)) AS industry,
        lower(trim(title)) AS title,
        trim(url) AS url
    FROM {{ source('public', 'common_crawl_data') }}
    WHERE title IS NOT NULL AND url IS NOT NULL
)
SELECT * FROM clean_common_crawl
