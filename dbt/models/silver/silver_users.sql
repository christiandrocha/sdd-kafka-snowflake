{{
    config(
        materialized = 'table',
        schema       = 'SILVER'
    )
}}

-- Silver: unified user entity from MongoDB and MSSQL sources.
-- FULL OUTER JOIN on CPF (normalized to 11 digits, digits only).
-- users_mongo wins on contact fields; users_mssql contributes extended profile.
-- materialized='table' because FULL OUTER JOIN is incompatible with incremental merge.

WITH mongo AS (
    SELECT
        uuid                                                     AS mongo_uuid,
        user_id                                                  AS mongo_user_id,
        REGEXP_REPLACE(cpf, '[^0-9]', '')                        AS cpf_normalized,
        email,
        phone_number,
        city,
        country,
        delivery_address
    FROM {{ ref('bronze_users_mongo') }}
),

mssql AS (
    SELECT
        uuid                                                     AS mssql_uuid,
        user_id                                                  AS mssql_user_id,
        REGEXP_REPLACE(cpf, '[^0-9]', '')                        AS cpf_normalized,
        first_name,
        last_name,
        phone_number                                             AS mssql_phone_number,
        birthday,
        job,
        company_name,
        country                                                  AS mssql_country
    FROM {{ ref('bronze_users_mssql') }}
),

joined AS (
    SELECT
        COALESCE(m.cpf_normalized, s.cpf_normalized)             AS cpf_normalized,
        COALESCE(m.mongo_uuid, s.mssql_uuid)                     AS uuid,
        COALESCE(m.mongo_user_id, s.mssql_user_id)               AS user_id,
        m.email,
        COALESCE(m.phone_number, s.mssql_phone_number)           AS phone_number,
        m.city,
        COALESCE(m.country, s.mssql_country)                     AS country,
        m.delivery_address,
        s.first_name,
        s.last_name,
        s.birthday,
        s.job,
        s.company_name,
        CASE
            WHEN m.cpf_normalized IS NOT NULL AND s.cpf_normalized IS NOT NULL THEN 'both'
            WHEN m.cpf_normalized IS NOT NULL THEN 'mongo'
            ELSE 'mssql'
        END                                                      AS source
    FROM mongo m
    FULL OUTER JOIN mssql s
        ON m.cpf_normalized = s.cpf_normalized
)

SELECT * FROM joined
