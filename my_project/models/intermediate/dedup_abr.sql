WITH deduplicated_abr AS (
    SELECT DISTINCT ON (abn)
        abn,
        entity_name,
        entity_type,
        entity_status,
        entity_address,
        entity_postcode,
        entity_state,
        entity_start_date
    FROM {{ ref('stg_abr_data') }}
    ORDER BY abn
)
SELECT * FROM deduplicated_abr
