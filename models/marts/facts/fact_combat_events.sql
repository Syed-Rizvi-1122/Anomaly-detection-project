{{
  config(
    materialized='table',
    description='Fact table for combat-related telemetry events (kills, damage, attacks)'
  )
}}

WITH combat_events AS (
    SELECT * 
    FROM {{ ref('stg_telemetry_events') }}
    WHERE event_type IN (
        'LogPlayerKillV2', 
        'LogPlayerAttack', 
        'LogPlayerTakeDamage', 
        'LogPlayerMakeGroggy',
        'LogPlayerRevive'
    )
),

combat_enriched AS (
    SELECT
        -- Fact table primary key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS combat_fact_key,
        
        -- Foreign keys to dimensions
        {{ dbt_utils.generate_surrogate_key(['character_account_id']) }} AS player_key,
        {{ dbt_utils.generate_surrogate_key(['match_id']) }} AS match_key,
        {{ dbt_utils.generate_surrogate_key(['location_x', 'location_y', 'location_z']) }} AS location_key,
        {{ dbt_utils.generate_surrogate_key(['DATE_TRUNC(\'hour\', event_timestamp)']) }} AS time_key,
        
        -- Event identifiers
        event_id,
        event_type,
        event_timestamp,
        match_id,
        character_account_id,
        character_name,
        team_id,
        
        -- Combat-specific measures
        damage,
        distance,
        attack_id,
        
        -- Location context
        location_x,
        location_y,
        location_z,
        
        -- Game state
        is_game,
        
        -- Combat event categorization
        CASE 
            WHEN event_type = 'LogPlayerKillV2' THEN 'Kill'
            WHEN event_type = 'LogPlayerAttack' THEN 'Attack'
            WHEN event_type = 'LogPlayerTakeDamage' THEN 'Damage'
            WHEN event_type = 'LogPlayerMakeGroggy' THEN 'Knockdown'
            WHEN event_type = 'LogPlayerRevive' THEN 'Revive'
            ELSE 'Other'
        END AS combat_action,
        
        -- Combat effectiveness measures
        CASE 
            WHEN event_type = 'LogPlayerKillV2' THEN 1 
            ELSE 0 
        END AS is_kill,
        
        CASE 
            WHEN event_type = 'LogPlayerTakeDamage' THEN 1 
            ELSE 0 
        END AS is_damage_taken,
        
        CASE 
            WHEN event_type = 'LogPlayerAttack' THEN 1 
            ELSE 0 
        END AS is_attack,
        
        CASE 
            WHEN event_type = 'LogPlayerMakeGroggy' THEN 1 
            ELSE 0 
        END AS is_knockdown,
        
        CASE 
            WHEN event_type = 'LogPlayerRevive' THEN 1 
            ELSE 0 
        END AS is_revive,
        
        -- Distance categorization
        CASE 
            WHEN distance IS NOT NULL THEN
                CASE 
                    WHEN distance <= 10 THEN 'Melee'
                    WHEN distance <= 50 THEN 'Close'
                    WHEN distance <= 100 THEN 'Medium'
                    WHEN distance <= 200 THEN 'Long'
                    WHEN distance <= 500 THEN 'Very Long'
                    ELSE 'Extreme'
                END
            ELSE 'Unknown'
        END AS engagement_range,
        
        -- Damage categorization
        CASE 
            WHEN damage IS NOT NULL THEN
                CASE 
                    WHEN damage <= 20 THEN 'Low'
                    WHEN damage <= 50 THEN 'Medium'
                    WHEN damage <= 80 THEN 'High'
                    ELSE 'Very High'
                END
            ELSE 'Unknown'
        END AS damage_category,
        
        -- Game phase context
        CASE 
            WHEN is_game = 0 THEN 'Pre-Game'
            WHEN is_game = 0.1 THEN 'Airplane'
            WHEN is_game = 0.5 THEN 'Lobby'
            WHEN is_game = 1.0 THEN 'Early Game'
            WHEN is_game BETWEEN 1.0 AND 2.0 THEN 'Mid Game'
            WHEN is_game > 2.0 THEN 'Late Game'
            ELSE 'Unknown'
        END AS game_phase,
        
        -- Combat intensity score
        CASE 
            WHEN event_type = 'LogPlayerKillV2' AND damage >= 100 THEN 5
            WHEN event_type = 'LogPlayerKillV2' AND damage >= 50 THEN 4
            WHEN event_type = 'LogPlayerKillV2' THEN 3
            WHEN event_type = 'LogPlayerMakeGroggy' THEN 2
            WHEN event_type = 'LogPlayerTakeDamage' AND damage >= 50 THEN 2
            WHEN event_type = 'LogPlayerTakeDamage' THEN 1
            WHEN event_type = 'LogPlayerRevive' THEN 1
            ELSE 0
        END AS combat_intensity_score,
        
        -- Location-based metrics
        CASE 
            WHEN location_x IS NOT NULL AND location_y IS NOT NULL THEN
                SQRT(POWER(location_x - 408000, 2) + POWER(location_y - 408000, 2))
            ELSE NULL
        END AS distance_from_map_center,
        
        -- Combat effectiveness ratios (for aggregation)
        CASE 
            WHEN event_type = 'LogPlayerKillV2' AND distance IS NOT NULL THEN damage / distance
            ELSE NULL
        END AS damage_per_distance,
        
        -- Time-based context
        EXTRACT(hour FROM event_timestamp) AS event_hour,
        EXTRACT(dayofweek FROM event_timestamp) AS event_day_of_week,
        
        -- Combat location zone
        CASE 
            WHEN location_x IS NOT NULL AND location_y IS NOT NULL THEN
                CASE 
                    WHEN location_x <= 204000 AND location_y <= 204000 THEN 'Northwest'
                    WHEN location_x <= 408000 AND location_y <= 204000 THEN 'North'
                    WHEN location_x <= 612000 AND location_y <= 204000 THEN 'Northeast'
                    WHEN location_x <= 816000 AND location_y <= 204000 THEN 'Far Northeast'
                    WHEN location_x <= 204000 AND location_y <= 408000 THEN 'West'
                    WHEN location_x <= 408000 AND location_y <= 408000 THEN 'Center'
                    WHEN location_x <= 612000 AND location_y <= 408000 THEN 'East'
                    WHEN location_x <= 816000 AND location_y <= 408000 THEN 'Far East'
                    WHEN location_x <= 204000 AND location_y <= 612000 THEN 'Southwest'
                    WHEN location_x <= 408000 AND location_y <= 612000 THEN 'South'
                    WHEN location_x <= 612000 AND location_y <= 612000 THEN 'Southeast'
                    ELSE 'Far Southeast'
                END
            ELSE 'Unknown'
        END AS combat_zone,
        
        -- Combat success flag
        CASE 
            WHEN event_type IN ('LogPlayerKillV2', 'LogPlayerMakeGroggy', 'LogPlayerRevive') THEN 1
            ELSE 0
        END AS successful_combat_action,
        
        -- Combat defensive flag
        CASE 
            WHEN event_type IN ('LogPlayerTakeDamage', 'LogPlayerRevive') THEN 1
            ELSE 0
        END AS defensive_combat_action,
        
        -- Combat offensive flag
        CASE 
            WHEN event_type IN ('LogPlayerKillV2', 'LogPlayerAttack', 'LogPlayerMakeGroggy') THEN 1
            ELSE 0
        END AS offensive_combat_action,
        
        -- Raw event data for advanced analytics
        event_data,
        
        -- Metadata
        ingested_at,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM combat_events
    WHERE event_timestamp IS NOT NULL
)

SELECT * FROM combat_enriched