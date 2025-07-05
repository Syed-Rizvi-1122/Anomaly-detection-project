{{
  config(
    materialized='table',
    description='Main fact table for PUBG telemetry events'
  )
}}

WITH telemetry_events AS (
    SELECT * FROM {{ ref('stg_telemetry_events') }}
),

final AS (
    SELECT
        -- Fact table primary key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS fact_key,
        
        -- Foreign keys to dimensions
        {{ dbt_utils.generate_surrogate_key(['character_account_id']) }} AS player_key,
        {{ dbt_utils.generate_surrogate_key(['match_id']) }} AS match_key,
        {{ dbt_utils.generate_surrogate_key(['item_id']) }} AS item_key,
        {{ dbt_utils.generate_surrogate_key(['location_x', 'location_y', 'location_z']) }} AS location_key,
        {{ dbt_utils.generate_surrogate_key(['DATE_TRUNC(\'hour\', event_timestamp)']) }} AS time_key,
        
        -- Event identifiers
        event_id,
        event_type,
        event_timestamp,
        match_id,
        character_account_id,
        
        -- Event measures
        damage,
        distance,
        attack_id,
        team_id,
        is_game,
        
        -- Location measures
        location_x,
        location_y,
        location_z,
        
        -- Item attributes
        item_id,
        item_category,
        item_subcategory,
        
        -- Vehicle attributes
        vehicle_id,
        vehicle_type,
        
        -- Character attributes
        character_name,
        
        -- Event type categorization
        CASE 
            WHEN event_type LIKE '%Player%' THEN 'Player'
            WHEN event_type LIKE '%Item%' THEN 'Item'
            WHEN event_type LIKE '%Vehicle%' THEN 'Vehicle'
            WHEN event_type LIKE '%Care%' THEN 'CarePackage'
            WHEN event_type LIKE '%Match%' THEN 'Match'
            WHEN event_type LIKE '%Phase%' THEN 'GamePhase'
            ELSE 'Other'
        END AS event_category,
        
        -- Event subcategory
        CASE 
            WHEN event_type IN ('LogPlayerKillV2', 'LogPlayerAttack', 'LogPlayerTakeDamage', 'LogPlayerMakeGroggy') THEN 'Combat'
            WHEN event_type IN ('LogPlayerPosition', 'LogPlayerCreate', 'LogPlayerRevive') THEN 'Movement'
            WHEN event_type IN ('LogItemPickup', 'LogItemDrop', 'LogItemEquip', 'LogItemUse') THEN 'Inventory'
            WHEN event_type IN ('LogVehicleRide', 'LogVehicleLeave', 'LogVehicleDamage') THEN 'Transportation'
            WHEN event_type IN ('LogMatchStart', 'LogMatchEnd', 'LogPhaseChange') THEN 'GameState'
            ELSE 'Other'
        END AS event_subcategory,
        
        -- Calculated measures
        CASE 
            WHEN location_x IS NOT NULL AND location_y IS NOT NULL THEN
                SQRT(POWER(location_x - 408000, 2) + POWER(location_y - 408000, 2))
            ELSE NULL
        END AS distance_from_map_center,
        
        -- Event flags
        CASE WHEN event_type = 'LogPlayerKillV2' THEN 1 ELSE 0 END AS is_kill_event,
        CASE WHEN event_type = 'LogPlayerTakeDamage' THEN 1 ELSE 0 END AS is_damage_event,
        CASE WHEN event_type LIKE '%Item%' THEN 1 ELSE 0 END AS is_item_event,
        CASE WHEN event_type LIKE '%Vehicle%' THEN 1 ELSE 0 END AS is_vehicle_event,
        CASE WHEN event_type = 'LogPlayerPosition' THEN 1 ELSE 0 END AS is_position_event,
        CASE WHEN event_type = 'LogHeal' THEN 1 ELSE 0 END AS is_heal_event,
        CASE WHEN event_type LIKE '%Pickup%' THEN 1 ELSE 0 END AS is_pickup_event,
        CASE WHEN event_type LIKE '%Drop%' THEN 1 ELSE 0 END AS is_drop_event,
        
        -- Combat specific measures
        CASE 
            WHEN event_type IN ('LogPlayerKillV2', 'LogPlayerAttack', 'LogPlayerTakeDamage', 'LogPlayerMakeGroggy') 
                AND distance IS NOT NULL THEN
                CASE 
                    WHEN distance <= 10 THEN 'Melee'
                    WHEN distance <= 50 THEN 'Close'
                    WHEN distance <= 100 THEN 'Medium'
                    WHEN distance <= 200 THEN 'Long'
                    ELSE 'Very Long'
                END
            ELSE NULL
        END AS engagement_range,
        
        -- Game phase indicator
        CASE 
            WHEN is_game = 0 THEN 'Pre-Game'
            WHEN is_game = 0.1 THEN 'Airplane'
            WHEN is_game = 0.5 THEN 'Lobby'
            WHEN is_game = 1.0 THEN 'Early Game'
            WHEN is_game BETWEEN 1.0 AND 2.0 THEN 'Mid Game'
            WHEN is_game > 2.0 THEN 'Late Game'
            ELSE 'Unknown'
        END AS game_phase,
        
        -- Performance metrics
        CASE 
            WHEN event_type = 'LogPlayerKillV2' AND damage IS NOT NULL THEN damage
            ELSE NULL
        END AS kill_damage,
        
        CASE 
            WHEN event_type = 'LogPlayerTakeDamage' AND damage IS NOT NULL THEN damage
            ELSE NULL
        END AS damage_taken,
        
        -- Time-based measures
        EXTRACT(hour FROM event_timestamp) AS event_hour,
        EXTRACT(dayofweek FROM event_timestamp) AS event_day_of_week,
        
        -- Raw event data for advanced analytics
        event_data,
        
        -- Data quality measures
        CASE 
            WHEN character_account_id IS NULL THEN 1 
            ELSE 0 
        END AS missing_player_flag,
        
        CASE 
            WHEN location_x IS NULL OR location_y IS NULL THEN 1 
            ELSE 0 
        END AS missing_location_flag,
        
        CASE 
            WHEN event_type LIKE '%Item%' AND item_id IS NULL THEN 1 
            ELSE 0 
        END AS missing_item_flag,
        
        -- Metadata
        ingested_at,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM telemetry_events
    WHERE event_timestamp IS NOT NULL
)

SELECT * FROM final