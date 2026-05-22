-- Fails if any cpf_normalized appears more than once in silver_users.
-- The FULL OUTER JOIN on CPF must produce exactly one row per user.
-- Duplicates indicate a data quality issue in the source CPF values.

SELECT
    cpf_normalized,
    COUNT(*) AS cnt
FROM {{ ref('silver_users') }}
GROUP BY cpf_normalized
HAVING cnt > 1
