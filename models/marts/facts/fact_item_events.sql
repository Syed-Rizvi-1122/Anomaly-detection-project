{{
  config(
    materialized='table',
    description='Fact table for item-related telemetry events (pickup, drop, equip, use)'
  )
}}

WITH item_events AS (
    SELECT * 
    FROM {{ ref('stg_telemetry_events') }}
    WHERE event_type LIKE '%Item%'
      AND item_id IS NOT NULL
),

item_enriched AS (
    SELECT
        -- Fact table primary key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS item_fact_key,
        
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
        character_name,
        team_id,
        
        -- Item-specific attributes
        item_id,
        item_category,
        item_subcategory,
        
        -- Location context
        location_x,
        location_y,
        location_z,
        
        -- Game state
        is_game,
        
        -- Item event categorization
        CASE 
            WHEN event_type = 'LogItemPickup' THEN 'Pickup'
            WHEN event_type = 'LogItemDrop' THEN 'Drop'
            WHEN event_type = 'LogItemEquip' THEN 'Equip'
            WHEN event_type = 'LogItemUnequip' THEN 'Unequip'
            WHEN event_type = 'LogItemUse' THEN 'Use'
            WHEN event_type = 'LogItemAttach' THEN 'Attach'
            WHEN event_type = 'LogItemDetach' THEN 'Detach'
            WHEN event_type LIKE '%Carepackage%' THEN 'CarePackage'
            WHEN event_type LIKE '%Lootbox%' THEN 'Lootbox'
            WHEN event_type LIKE '%VehicleTrunk%' THEN 'VehicleTrunk'
            ELSE 'Other'
        END AS item_action,
        
        -- Item event measures
        CASE 
            WHEN event_type = 'LogItemPickup' THEN 1 
            ELSE 0 
        END AS is_pickup,
        
        CASE 
            WHEN event_type = 'LogItemDrop' THEN 1 
            ELSE 0 
        END AS is_drop,
        
        CASE 
            WHEN event_type = 'LogItemEquip' THEN 1 
            ELSE 0 
        END AS is_equip,
        
        CASE 
            WHEN event_type = 'LogItemUnequip' THEN 1 
            ELSE 0 
        END AS is_unequip,
        
        CASE 
            WHEN event_type = 'LogItemUse' THEN 1 
            ELSE 0 
        END AS is_use,
        
        CASE 
            WHEN event_type = 'LogItemAttach' THEN 1 
            ELSE 0 
        END AS is_attach,
        
        CASE 
            WHEN event_type = 'LogItemDetach' THEN 1 
            ELSE 0 
        END AS is_detach,
        
        CASE 
            WHEN event_type LIKE '%Carepackage%' THEN 1 
            ELSE 0 
        END AS is_carepackage_event,
        
        CASE 
            WHEN event_type LIKE '%Lootbox%' THEN 1 
            ELSE 0 
        END AS is_lootbox_event,
        
        CASE 
            WHEN event_type LIKE '%VehicleTrunk%' THEN 1 
            ELSE 0 
        END AS is_vehicle_trunk_event,
        
        -- Item type classification
        CASE 
            WHEN LOWER(item_category) = 'weapon' THEN 'Weapon'
            WHEN LOWER(item_category) = 'equipment' THEN 'Equipment'
            WHEN LOWER(item_category) = 'attachment' THEN 'Attachment'
            WHEN LOWER(item_category) = 'use' THEN 'Consumable'
            WHEN LOWER(item_category) = 'boost' THEN 'Boost'
            WHEN LOWER(item_category) = 'heal' THEN 'Healing'
            WHEN LOWER(item_category) = 'throwable' THEN 'Throwable'
            ELSE 'Other'
        END AS item_type,
        
        -- Item value/rarity estimation
        CASE 
            WHEN LOWER(item_category) = 'weapon' AND LOWER(item_subcategory) = 'main' THEN 'High'
            WHEN LOWER(item_category) = 'equipment' THEN 'Medium'
            WHEN LOWER(item_category) = 'attachment' THEN 'Medium'
            WHEN LOWER(item_category) IN ('heal', 'boost') THEN 'Medium'
            WHEN LOWER(item_category) = 'use' THEN 'Low'
            WHEN LOWER(item_category) = 'throwable' THEN 'Low'
            ELSE 'Unknown'
        END AS item_value_tier,
        
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
        
        -- Item management efficiency score
        CASE 
            WHEN event_type = 'LogItemPickup' AND LOWER(item_category) = 'weapon' THEN 5
            WHEN event_type = 'LogItemPickup' AND LOWER(item_category) = 'equipment' THEN 4
            WHEN event_type = 'LogItemPickup' AND LOWER(item_category) = 'attachment' THEN 3
            WHEN event_type = 'LogItemPickup' AND LOWER(item_category) IN ('heal', 'boost') THEN 3
            WHEN event_type = 'LogItemPickup' THEN 2
            WHEN event_type = 'LogItemEquip' THEN 3
            WHEN event_type = 'LogItemUse' AND LOWER(item_category) IN ('heal', 'boost') THEN 4
            WHEN event_type = 'LogItemUse' THEN 2
            WHEN event_type = 'LogItemAttach' THEN 2
            WHEN event_type = 'LogItemDrop' THEN 1
            ELSE 0
        END AS item_management_score,
        
        -- Location-based metrics
        CASE 
            WHEN location_x IS NOT NULL AND location_y IS NOT NULL THEN
                SQRT(POWER(location_x - 408000, 2) + POWER(location_y - 408000, 2))
            ELSE NULL
        END AS distance_from_map_center,
        
        -- Item acquisition context
        CASE 
            WHEN event_type = 'LogItemPickupFromCarepackage' THEN 'CarePackage'
            WHEN event_type = 'LogItemPickupFromLootbox' THEN 'Lootbox'
            WHEN event_type = 'LogItemPickupFromVehicleTrunk' THEN 'VehicleTrunk'
            WHEN event_type = 'LogItemPickupFromCustomPackage' THEN 'CustomPackage'
            WHEN event_type = 'LogItemPickup' THEN 'Ground'
            ELSE 'Unknown'
        END AS item_source,
        
        -- Time-based context
        EXTRACT(hour FROM event_timestamp) AS event_hour,
        EXTRACT(dayofweek FROM event_timestamp) AS event_day_of_week,
        
        -- Item location zone
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
        END AS item_zone,
        
        -- Item management behavior flags
        CASE 
            WHEN event_type IN ('LogItemPickup', 'LogItemEquip', 'LogItemUse', 'LogItemAttach') THEN 1
            ELSE 0
        END AS positive_item_action,
        
        CASE 
            WHEN event_type IN ('LogItemDrop', 'LogItemUnequip', 'LogItemDetach') THEN 1
            ELSE 0
        END AS negative_item_action,
        
        -- Weapon-specific flags
        CASE 
            WHEN LOWER(item_category) = 'weapon' THEN 1
            ELSE 0
        END AS is_weapon_event,
        
        -- Consumable-specific flags
        CASE 
            WHEN LOWER(item_category) IN ('heal', 'boost', 'use') THEN 1
            ELSE 0
        END AS is_consumable_event,
        
        -- Attachment-specific flags
        CASE 
            WHEN LOWER(item_category) = 'attachment' THEN 1
            ELSE 0
        END AS is_attachment_event,
        
        -- Equipment-specific flags
        CASE 
            WHEN LOWER(item_category) = 'equipment' THEN 1
            ELSE 0
        END AS is_equipment_event,
        
        -- Special event flags
        CASE 
            WHEN event_type LIKE '%Carepackage%' THEN 1
            ELSE 0
        END AS is_special_item_event,
        
        -- Raw event data for advanced analytics
        event_data,
        
        -- Metadata
        ingested_at,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM item_events
    WHERE event_timestamp IS NOT NULL
)

SELECT * FROM item_enriched