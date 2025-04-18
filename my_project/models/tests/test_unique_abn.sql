-- Test uniqueness for ABN
SELECT abn
FROM {{ ref('dedup_abr') }}
GROUP BY abn
HAVING COUNT(*) > 1


