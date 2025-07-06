{{
  config(
    materialized='view',
    description='Staging model for PUBG telemetry events with cleaned and standardized data'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('pubg_raw', 'telemetry_events') }}
),

cleaned_events AS (
    SELECT
        _D::timestamp AS event_timestamp,
        _T AS event_type,
        match_id,
        
        -- Common fields extraction
        common:isGame::float AS is_game,
        
        -- Event-specific data extraction based on event type
        CASE 
            WHEN _T LIKE '%Player%' THEN event_data:character:accountId::varchar
            WHEN _T LIKE '%LogItemPickup%' THEN event_data:character:accountId::varchar
            WHEN _T LIKE '%LogVehicle%' THEN event_data:character:accountId::varchar
            ELSE NULL
        END AS character_account_id,
        
        CASE 
            WHEN _T LIKE '%Player%' THEN event_data:character:name::varchar
            WHEN _T LIKE '%LogItemPickup%' THEN event_data:character:name::varchar
            WHEN _T LIKE '%LogVehicle%' THEN event_data:character:name::varchar
            ELSE NULL
        END AS character_name,
        
        CASE 
            WHEN _T LIKE '%Player%' THEN event_data:character:teamId::integer
            WHEN _T LIKE '%LogItemPickup%' THEN event_data:character:teamId::integer
            WHEN _T LIKE '%LogVehicle%' THEN event_data:character:teamId::integer
            ELSE NULL
        END AS team_id,
        
        -- Location data
        CASE 
            WHEN _T LIKE '%Player%' THEN event_data:character:location:x::float
            WHEN _T LIKE '%LogItemPickup%' THEN event_data:character:location:x::float
            WHEN _T LIKE '%LogVehicle%' THEN event_data:character:location:x::float
            ELSE NULL
        END AS location_x,
        
        CASE 
            WHEN _T LIKE '%Player%' THEN event_data:character:location:y::float
            WHEN _T LIKE '%LogItemPickup%' THEN event_data:character:location:y::float
            WHEN _T LIKE '%LogVehicle%' THEN event_data:character:location:y::float
            ELSE NULL
        END AS location_y,
        
        CASE 
            WHEN _T LIKE '%Player%' THEN event_data:character:location:z::float
            WHEN _T LIKE '%LogItemPickup%' THEN event_data:character:location:z::float
            WHEN _T LIKE '%LogVehicle%' THEN event_data:character:location:z::float
            ELSE NULL
        END AS location_z,
        
        -- Item data
        CASE 
            WHEN _T LIKE '%Item%' THEN event_data:item:itemId::varchar
            ELSE NULL
        END AS item_id,
        
        CASE 
            WHEN _T LIKE '%Item%' THEN event_data:item:category::varchar
            ELSE NULL
        END AS item_category,
        
        CASE 
            WHEN _T LIKE '%Item%' THEN event_data:item:subCategory::varchar
            ELSE NULL
        END AS item_subcategory,
        
        -- Vehicle data
        CASE 
            WHEN _T LIKE '%Vehicle%' THEN event_data:vehicle:vehicleId::varchar
            ELSE NULL
        END AS vehicle_id,
        
        CASE 
            WHEN _T LIKE '%Vehicle%' THEN event_data:vehicle:vehicleType::varchar
            ELSE NULL
        END AS vehicle_type,
        
        -- Combat data
        CASE 
            WHEN _T IN ('LogPlayerKillV2', 'LogPlayerAttack', 'LogPlayerTakeDamage', 'LogPlayerMakeGroggy') 
            THEN event_data:attackId::integer
            ELSE NULL
        END AS attack_id,
        
        CASE 
            WHEN _T IN ('LogPlayerKillV2', 'LogPlayerTakeDamage', 'LogPlayerMakeGroggy') 
            THEN event_data:damage::float
            ELSE NULL
        END AS damage,
        
        CASE 
            WHEN _T IN ('LogPlayerKillV2', 'LogPlayerAttack', 'LogPlayerTakeDamage', 'LogPlayerMakeGroggy') 
            THEN event_data:distance::float
            ELSE NULL
        END AS distance,
        
        -- Raw event data for complex parsing
        event_data,
        
        -- Metadata
        ingested_at,
        
        -- Create a unique event identifier
        {{ dbt_utils.generate_surrogate_key(['_D', '_T', 'match_id']) }} AS event_id
        
    FROM source_data
    WHERE _D IS NOT NULL
      AND _T IS NOT NULL
      AND match_id IS NOT NULL
)

SELECT * FROM cleaned_events