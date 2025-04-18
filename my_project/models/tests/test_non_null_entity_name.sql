-- Test non-null entity_name


SELECT *
FROM {{ ref('dedup_abr') }}
WHERE entity_name IS NULL
