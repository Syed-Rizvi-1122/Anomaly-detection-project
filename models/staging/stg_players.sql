{{
  config(
    materialized='view',
    description='Staging model for PUBG players with cleaned and standardized data'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('pubg_raw', 'players') }}
),

cleaned_players AS (
    SELECT
        account_id,
        player_name,
        platform,
        region,
        ingested_at,
        
        -- Standardize platform names
        CASE 
            WHEN LOWER(platform) = 'steam' THEN 'Steam'
            WHEN LOWER(platform) = 'xbox' THEN 'Xbox'
            WHEN LOWER(platform) = 'psn' THEN 'PlayStation'
            WHEN LOWER(platform) = 'console' THEN 'Console'
            ELSE platform
        END AS standardized_platform,
        
        -- Standardize region codes
        CASE 
            WHEN LOWER(region) = 'as' THEN 'Asia'
            WHEN LOWER(region) = 'eu' THEN 'Europe'
            WHEN LOWER(region) = 'na' THEN 'North America'
            WHEN LOWER(region) = 'oc' THEN 'Oceania'
            WHEN LOWER(region) = 'sa' THEN 'South America'
            WHEN LOWER(region) = 'sea' THEN 'South East Asia'
            WHEN LOWER(region) = 'krjp' THEN 'Korea/Japan'
            WHEN LOWER(region) = 'ru' THEN 'Russia'
            WHEN LOWER(region) = 'kakao' THEN 'Kakao'
            WHEN LOWER(region) = 'jp' THEN 'Japan'
            ELSE region
        END AS standardized_region,
        
        -- Create player dimension key
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS player_key,
        
        -- Clean player name
        TRIM(player_name) AS cleaned_player_name,
        
        -- Determine platform type
        CASE 
            WHEN LOWER(platform) = 'steam' THEN 'PC'
            WHEN LOWER(platform) IN ('xbox', 'psn') THEN 'Console'
            ELSE 'Other'
        END AS platform_type,
        
        -- Extract player name length for analytics
        LENGTH(TRIM(player_name)) AS player_name_length,
        
        -- Check if player name contains special characters
        CASE 
            WHEN REGEXP_LIKE(player_name, '[^a-zA-Z0-9_-]') THEN TRUE
            ELSE FALSE
        END AS has_special_chars,
        
        -- Determine player registration recency
        CASE 
            WHEN ingested_at >= CURRENT_DATE - 30 THEN 'New'
            WHEN ingested_at >= CURRENT_DATE - 90 THEN 'Recent'
            ELSE 'Established'
        END AS player_recency
        
    FROM source_data
    WHERE account_id IS NOT NULL
      AND player_name IS NOT NULL
)

SELECT * FROM cleaned_players