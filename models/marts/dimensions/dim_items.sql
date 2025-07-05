{{
  config(
    materialized='table',
    description='Item dimension table containing all item attributes'
  )
}}

WITH item_base AS (
    SELECT DISTINCT
        item_id,
        item_category,
        item_subcategory
    FROM {{ ref('stg_telemetry_events') }}
    WHERE item_id IS NOT NULL
),

item_stats AS (
    SELECT 
        item_id,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN event_type = 'LogItemPickup' THEN 1 END) AS pickup_count,
        COUNT(CASE WHEN event_type = 'LogItemDrop' THEN 1 END) AS drop_count,
        COUNT(CASE WHEN event_type = 'LogItemEquip' THEN 1 END) AS equip_count,
        COUNT(CASE WHEN event_type = 'LogItemUse' THEN 1 END) AS use_count,
        COUNT(DISTINCT character_account_id) AS unique_users,
        COUNT(DISTINCT match_id) AS matches_appeared
    FROM {{ ref('stg_telemetry_events') }}
    WHERE item_id IS NOT NULL
    GROUP BY item_id
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['i.item_id']) }} AS item_key,
        i.item_id,
        i.item_category,
        i.item_subcategory,
        
        -- Item statistics
        COALESCE(s.total_events, 0) AS total_events,
        COALESCE(s.pickup_count, 0) AS pickup_count,
        COALESCE(s.drop_count, 0) AS drop_count,
        COALESCE(s.equip_count, 0) AS equip_count,
        COALESCE(s.use_count, 0) AS use_count,
        COALESCE(s.unique_users, 0) AS unique_users,
        COALESCE(s.matches_appeared, 0) AS matches_appeared,
        
        -- Item popularity metrics
        CASE 
            WHEN s.pickup_count >= 1000 THEN 'Very Popular'
            WHEN s.pickup_count >= 500 THEN 'Popular'
            WHEN s.pickup_count >= 100 THEN 'Moderate'
            WHEN s.pickup_count >= 10 THEN 'Uncommon'
            ELSE 'Rare'
        END AS popularity_tier,
        
        -- Item type classification
        CASE 
            WHEN LOWER(i.item_category) = 'weapon' THEN 'Weapon'
            WHEN LOWER(i.item_category) = 'equipment' THEN 'Equipment'
            WHEN LOWER(i.item_category) = 'attachment' THEN 'Attachment'
            WHEN LOWER(i.item_category) = 'use' THEN 'Consumable'
            WHEN LOWER(i.item_category) = 'boost' THEN 'Boost'
            WHEN LOWER(i.item_category) = 'heal' THEN 'Healing'
            WHEN LOWER(i.item_category) = 'throwable' THEN 'Throwable'
            ELSE 'Other'
        END AS item_type,
        
        -- Weapon subcategory classification
        CASE 
            WHEN LOWER(i.item_subcategory) IN ('main', 'handgun', 'melee') THEN 'Primary'
            WHEN LOWER(i.item_subcategory) IN ('none', 'special') THEN 'Secondary'
            ELSE 'Utility'
        END AS weapon_classification,
        
        -- Usage frequency
        CASE 
            WHEN s.use_count::float / NULLIF(s.pickup_count, 0) >= 0.8 THEN 'High Usage'
            WHEN s.use_count::float / NULLIF(s.pickup_count, 0) >= 0.5 THEN 'Medium Usage'
            WHEN s.use_count::float / NULLIF(s.pickup_count, 0) >= 0.2 THEN 'Low Usage'
            ELSE 'Minimal Usage'
        END AS usage_frequency,
        
        -- Item retention (equip vs pickup ratio)
        CASE 
            WHEN s.equip_count::float / NULLIF(s.pickup_count, 0) >= 0.8 THEN 'High Retention'
            WHEN s.equip_count::float / NULLIF(s.pickup_count, 0) >= 0.5 THEN 'Medium Retention'
            WHEN s.equip_count::float / NULLIF(s.pickup_count, 0) >= 0.2 THEN 'Low Retention'
            ELSE 'Minimal Retention'
        END AS item_retention,
        
        -- Item rarity based on match appearance
        CASE 
            WHEN s.matches_appeared::float / (SELECT COUNT(DISTINCT match_id) FROM {{ ref('stg_telemetry_events') }}) >= 0.8 THEN 'Common'
            WHEN s.matches_appeared::float / (SELECT COUNT(DISTINCT match_id) FROM {{ ref('stg_telemetry_events') }}) >= 0.5 THEN 'Uncommon'
            WHEN s.matches_appeared::float / (SELECT COUNT(DISTINCT match_id) FROM {{ ref('stg_telemetry_events') }}) >= 0.2 THEN 'Rare'
            ELSE 'Very Rare'
        END AS item_rarity,
        
        -- Clean item name (remove prefixes and suffixes)
        CASE 
            WHEN i.item_id LIKE 'Item_%' THEN 
                REPLACE(
                    REPLACE(
                        REPLACE(i.item_id, 'Item_', ''),
                        '_C', ''
                    ),
                    '_', ' '
                )
            ELSE i.item_id
        END AS clean_item_name,
        
        -- Is attachment flag
        CASE 
            WHEN LOWER(i.item_category) = 'attachment' THEN TRUE
            ELSE FALSE
        END AS is_attachment,
        
        -- Is weapon flag
        CASE 
            WHEN LOWER(i.item_category) = 'weapon' THEN TRUE
            ELSE FALSE
        END AS is_weapon,
        
        -- Is consumable flag
        CASE 
            WHEN LOWER(i.item_category) IN ('use', 'heal', 'boost') THEN TRUE
            ELSE FALSE
        END AS is_consumable,
        
        -- Record metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM item_base i
    LEFT JOIN item_stats s ON i.item_id = s.item_id
)

SELECT * FROM final