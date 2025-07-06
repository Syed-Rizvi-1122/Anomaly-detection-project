{{
  config(
    materialized='table',
    description='Player dimension table containing all player attributes'
  )
}}

WITH player_base AS (
    SELECT * FROM {{ ref('stg_players') }}
),

player_stats AS (
    SELECT 
        character_account_id,
        COUNT(DISTINCT match_id) AS total_matches,
        MIN(event_timestamp) AS first_event_date,
        MAX(event_timestamp) AS last_event_date,
        COUNT(CASE WHEN event_type = 'LogPlayerKillV2' THEN 1 END) AS total_kills,
        COUNT(CASE WHEN event_type = 'LogPlayerTakeDamage' THEN 1 END) AS total_damage_events,
        COUNT(CASE WHEN event_type = 'LogItemPickup' THEN 1 END) AS total_item_pickups
    FROM {{ ref('stg_telemetry_events') }}
    WHERE character_account_id IS NOT NULL
    GROUP BY character_account_id
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['p.account_id']) }} AS player_key,
        p.account_id,
        p.cleaned_player_name AS player_name,
        p.standardized_platform AS platform,
        p.standardized_region AS region,
        p.platform_type,
        p.player_name_length,
        p.has_special_chars,
        p.player_recency,
        p.ingested_at,
        
        -- Player statistics
        COALESCE(s.total_matches, 0) AS total_matches,
        COALESCE(s.total_kills, 0) AS total_kills,
        COALESCE(s.total_damage_events, 0) AS total_damage_events,
        COALESCE(s.total_item_pickups, 0) AS total_item_pickups,
        s.first_event_date,
        s.last_event_date,
        
        -- Calculated metrics
        CASE 
            WHEN s.total_matches > 0 THEN s.total_kills::float / s.total_matches
            ELSE 0
        END AS avg_kills_per_match,
        
        CASE 
            WHEN s.total_matches > 0 THEN s.total_damage_events::float / s.total_matches
            ELSE 0
        END AS avg_damage_events_per_match,
        
        -- Player activity level
        CASE 
            WHEN s.total_matches >= 100 THEN 'High'
            WHEN s.total_matches >= 20 THEN 'Medium'
            WHEN s.total_matches >= 5 THEN 'Low'
            ELSE 'Minimal'
        END AS activity_level,
        
        -- Player skill level (based on kill rate)
        CASE 
            WHEN s.total_kills::float / NULLIF(s.total_matches, 0) >= 3 THEN 'Expert'
            WHEN s.total_kills::float / NULLIF(s.total_matches, 0) >= 1.5 THEN 'Advanced'
            WHEN s.total_kills::float / NULLIF(s.total_matches, 0) >= 0.5 THEN 'Intermediate'
            ELSE 'Beginner'
        END AS skill_level,
        
        -- Data quality flags
        CASE 
            WHEN p.player_name IS NULL THEN 'Missing Name'
            WHEN p.platform IS NULL THEN 'Missing Platform'
            WHEN p.region IS NULL THEN 'Missing Region'
            ELSE 'Complete'
        END AS data_quality_flag,
        
        -- Record metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM player_base p
    LEFT JOIN player_stats s ON p.account_id = s.character_account_id
)

SELECT * FROM final