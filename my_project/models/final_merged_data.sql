{{ config(materialized='table') }}

WITH abr_ranked AS (
    SELECT *, ROW_NUMBER() OVER () AS rn
    FROM {{ ref('dedup_abr') }}
),
crawl_ranked AS (
    SELECT *, ROW_NUMBER() OVER () AS rn
    FROM {{ ref('dedup_common_crawl') }}
)

SELECT
    a.abn,
    a.entity_name,
    a.entity_type,
    a.entity_status,
    a.entity_address,
    a.entity_postcode,
    a.entity_state,
    a.entity_start_date,
    c.industry,
    c.title,
    c.url
FROM abr_ranked a
JOIN crawl_ranked c
ON a.rn = c.rn
