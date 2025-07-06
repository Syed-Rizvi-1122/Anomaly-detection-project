{{
  config(
    materialized='view',
    description='Staging model for PUBG matches with cleaned and standardized data'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('pubg_raw', 'matches') }}
),

cleaned_matches AS (
    SELECT
        match_id,
        map_name,
        game_mode,
        match_type,
        created_at,
        duration,
        telemetry_url,
        ingested_at,
        
        -- Standardize map names
        CASE 
            WHEN LOWER(map_name) LIKE '%erangel%' THEN 'Erangel'
            WHEN LOWER(map_name) LIKE '%miramar%' THEN 'Miramar'
            WHEN LOWER(map_name) LIKE '%sanhok%' THEN 'Sanhok'
            WHEN LOWER(map_name) LIKE '%vikendi%' THEN 'Vikendi'
            WHEN LOWER(map_name) LIKE '%karakin%' THEN 'Karakin'
            WHEN LOWER(map_name) LIKE '%paramo%' THEN 'Paramo'
            WHEN LOWER(map_name) LIKE '%taego%' THEN 'Taego'
            WHEN LOWER(map_name) LIKE '%deston%' THEN 'Deston'
            WHEN LOWER(map_name) LIKE '%haven%' THEN 'Haven'
            ELSE map_name
        END AS standardized_map_name,
        
        -- Standardize game modes
        CASE 
            WHEN LOWER(game_mode) LIKE '%solo%' THEN 'Solo'
            WHEN LOWER(game_mode) LIKE '%duo%' THEN 'Duo'
            WHEN LOWER(game_mode) LIKE '%squad%' THEN 'Squad'
            WHEN LOWER(game_mode) LIKE '%team%' THEN 'Team'
            ELSE game_mode
        END AS standardized_game_mode,
        
        -- Calculate match duration in minutes
        CASE 
            WHEN duration > 0 THEN duration / 60.0
            ELSE NULL
        END AS duration_minutes,
        
        -- Extract date from match creation
        created_at::date AS match_date,
        
        -- Extract hour from match creation
        EXTRACT(hour FROM created_at) AS match_hour,
        
        -- Determine match period of day
        CASE 
            WHEN EXTRACT(hour FROM created_at) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(hour FROM created_at) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(hour FROM created_at) BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS match_period,
        
        -- Determine if match is recent (within last 7 days)
        CASE 
            WHEN created_at >= CURRENT_DATE - 7 THEN TRUE
            ELSE FALSE
        END AS is_recent_match,
        
        -- Create match dimension key
        {{ dbt_utils.generate_surrogate_key(['match_id']) }} AS match_key
        
    FROM source_data
    WHERE match_id IS NOT NULL
)

SELECT * FROM cleaned_matches