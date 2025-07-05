{{
  config(
    materialized='table',
    description='Match dimension table containing all match attributes'
  )
}}

WITH match_base AS (
    SELECT * FROM {{ ref('stg_matches') }}
),

match_stats AS (
    SELECT 
        match_id,
        COUNT(DISTINCT character_account_id) AS total_players,
        COUNT(DISTINCT team_id) AS total_teams,
        COUNT(*) AS total_events,
        MIN(event_timestamp) AS match_start_time,
        MAX(event_timestamp) AS match_end_time,
        COUNT(CASE WHEN event_type = 'LogPlayerKillV2' THEN 1 END) AS total_kills,
        COUNT(CASE WHEN event_type = 'LogItemPickup' THEN 1 END) AS total_item_pickups,
        COUNT(CASE WHEN event_type LIKE '%Vehicle%' THEN 1 END) AS total_vehicle_events
    FROM {{ ref('stg_telemetry_events') }}
    WHERE match_id IS NOT NULL
    GROUP BY match_id
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['m.match_id']) }} AS match_key,
        m.match_id,
        m.standardized_map_name AS map_name,
        m.standardized_game_mode AS game_mode,
        m.match_type,
        m.created_at,
        m.duration,
        m.duration_minutes,
        m.match_date,
        m.match_hour,
        m.match_period,
        m.is_recent_match,
        m.telemetry_url,
        m.ingested_at,
        
        -- Match statistics
        COALESCE(s.total_players, 0) AS total_players,
        COALESCE(s.total_teams, 0) AS total_teams,
        COALESCE(s.total_events, 0) AS total_events,
        COALESCE(s.total_kills, 0) AS total_kills,
        COALESCE(s.total_item_pickups, 0) AS total_item_pickups,
        COALESCE(s.total_vehicle_events, 0) AS total_vehicle_events,
        s.match_start_time,
        s.match_end_time,
        
        -- Calculated match duration from telemetry
        CASE 
            WHEN s.match_end_time IS NOT NULL AND s.match_start_time IS NOT NULL 
            THEN DATEDIFF(minute, s.match_start_time, s.match_end_time)
            ELSE m.duration_minutes
        END AS actual_duration_minutes,
        
        -- Match intensity metrics
        CASE 
            WHEN s.total_kills::float / NULLIF(s.total_players, 0) >= 0.8 THEN 'High Intensity'
            WHEN s.total_kills::float / NULLIF(s.total_players, 0) >= 0.5 THEN 'Medium Intensity'
            WHEN s.total_kills::float / NULLIF(s.total_players, 0) >= 0.2 THEN 'Low Intensity'
            ELSE 'Minimal Intensity'
        END AS match_intensity,
        
        -- Match size category
        CASE 
            WHEN s.total_players >= 80 THEN 'Large'
            WHEN s.total_players >= 50 THEN 'Medium'
            WHEN s.total_players >= 20 THEN 'Small'
            ELSE 'Tiny'
        END AS match_size_category,
        
        -- Match duration category
        CASE 
            WHEN m.duration_minutes >= 30 THEN 'Long'
            WHEN m.duration_minutes >= 20 THEN 'Medium'
            WHEN m.duration_minutes >= 10 THEN 'Short'
            ELSE 'Very Short'
        END AS match_duration_category,
        
        -- Map size category (based on coordinate ranges from documentation)
        CASE 
            WHEN m.standardized_map_name IN ('Erangel', 'Miramar', 'Taego', 'Vikendi', 'Deston') THEN 'Large'
            WHEN m.standardized_map_name = 'Sanhok' THEN 'Medium'
            WHEN m.standardized_map_name = 'Paramo' THEN 'Small'
            WHEN m.standardized_map_name IN ('Karakin', 'Range') THEN 'Tiny'
            WHEN m.standardized_map_name = 'Haven' THEN 'Mini'
            ELSE 'Unknown'
        END AS map_size_category,
        
        -- Weekend flag
        CASE 
            WHEN DAYOFWEEK(m.match_date) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        
        -- Peak hours flag
        CASE 
            WHEN m.match_hour BETWEEN 18 AND 23 THEN TRUE
            ELSE FALSE
        END AS is_peak_hours,
        
        -- Season indicator (based on match date)
        CASE 
            WHEN MONTH(m.match_date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(m.match_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(m.match_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(m.match_date) IN (9, 10, 11) THEN 'Fall'
            ELSE 'Unknown'
        END AS season,
        
        -- Data quality flags
        CASE 
            WHEN m.map_name IS NULL THEN 'Missing Map'
            WHEN m.game_mode IS NULL THEN 'Missing Game Mode'
            WHEN m.duration <= 0 THEN 'Invalid Duration'
            WHEN s.total_players = 0 THEN 'No Players'
            ELSE 'Complete'
        END AS data_quality_flag,
        
        -- Record metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM match_base m
    LEFT JOIN match_stats s ON m.match_id = s.match_id
)

SELECT * FROM final