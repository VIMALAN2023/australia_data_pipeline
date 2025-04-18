-- Clean and normalize abr_data
WITH clean_abr AS (
    SELECT
        trim(abn) AS abn,
        lower(trim(entity_name)) AS entity_name,
        lower(trim(entity_type)) AS entity_type,
        lower(trim(entity_status)) AS entity_status,
        lower(trim(entity_address)) AS entity_address,
        trim(entity_postcode) AS entity_postcode,
        trim(entity_state) AS entity_state,
        entity_start_date
    FROM {{ source('public', 'abr_data') }}
    WHERE abn IS NOT NULL AND entity_name IS NOT NULL
)
SELECT * FROM clean_abr
