{{
  config(
    materialized='table',
    description='Player performance summary by match using the star schema'
  )
}}

WITH player_match_events AS (
    SELECT
        f.match_id,
        f.character_account_id,
        p.player_name,
        p.platform,
        p.skill_level,
        m.map_name,
        m.game_mode,
        m.match_date,
        m.duration_minutes,
        
        -- Combat metrics
        COUNT(CASE WHEN f.event_category = 'Combat' THEN 1 END) AS combat_events,
        SUM(f.is_kill_event) AS kills,
        SUM(f.is_damage_event) AS damage_events_taken,
        SUM(CASE WHEN f.event_type = 'LogPlayerAttack' THEN 1 END) AS attacks_made,
        AVG(CASE WHEN f.damage IS NOT NULL THEN f.damage END) AS avg_damage_per_event,
        SUM(CASE WHEN f.damage IS NOT NULL THEN f.damage END) AS total_damage,
        
        -- Item management metrics
        COUNT(CASE WHEN f.event_category = 'Item' THEN 1 END) AS item_events,
        SUM(f.is_pickup_event) AS items_picked_up,
        SUM(f.is_drop_event) AS items_dropped,
        
        -- Movement and positioning metrics
        COUNT(CASE WHEN f.is_position_event = 1 THEN 1 END) AS position_updates,
        AVG(f.distance_from_map_center) AS avg_distance_from_center,
        COUNT(DISTINCT l.map_zone) AS zones_visited,
        
        -- Survival metrics
        SUM(f.is_heal_event) AS heals_used,
        
        -- Time-based metrics
        MIN(f.event_timestamp) AS first_event_time,
        MAX(f.event_timestamp) AS last_event_time,
        
        -- Game phase distribution
        COUNT(CASE WHEN f.game_phase = 'Early Game' THEN 1 END) AS early_game_events,
        COUNT(CASE WHEN f.game_phase = 'Mid Game' THEN 1 END) AS mid_game_events,
        COUNT(CASE WHEN f.game_phase = 'Late Game' THEN 1 END) AS late_game_events,
        
        -- Combat effectiveness
        CASE 
            WHEN SUM(CASE WHEN f.event_type = 'LogPlayerAttack' THEN 1 END) > 0 
            THEN SUM(f.is_kill_event)::float / SUM(CASE WHEN f.event_type = 'LogPlayerAttack' THEN 1 END)
            ELSE 0
        END AS kill_to_attack_ratio,
        
        -- Engagement ranges
        COUNT(CASE WHEN f.engagement_range = 'Close' THEN 1 END) AS close_range_engagements,
        COUNT(CASE WHEN f.engagement_range = 'Medium' THEN 1 END) AS medium_range_engagements,
        COUNT(CASE WHEN f.engagement_range = 'Long' THEN 1 END) AS long_range_engagements
        
    FROM {{ ref('fact_telemetry_events') }} f
    LEFT JOIN {{ ref('dim_players') }} p ON f.player_key = p.player_key
    LEFT JOIN {{ ref('dim_matches') }} m ON f.match_key = m.match_key  
    LEFT JOIN {{ ref('dim_locations') }} l ON f.location_key = l.location_key
    GROUP BY 
        f.match_id,
        f.character_account_id,
        p.player_name,
        p.platform,
        p.skill_level,
        m.map_name,
        m.game_mode,
        m.match_date,
        m.duration_minutes
),

enriched_summary AS (
    SELECT
        *,
        
        -- Calculated performance metrics
        CASE 
            WHEN last_event_time IS NOT NULL AND first_event_time IS NOT NULL
            THEN DATEDIFF(minute, first_event_time, last_event_time)
            ELSE NULL
        END AS survival_time_minutes,
        
        -- Activity level
        CASE 
            WHEN combat_events + item_events >= 100 THEN 'Very Active'
            WHEN combat_events + item_events >= 50 THEN 'Active'
            WHEN combat_events + item_events >= 20 THEN 'Moderate'
            ELSE 'Low Activity'
        END AS activity_level,
        
        -- Combat performance tier
        CASE 
            WHEN kills >= 10 THEN 'Elite'
            WHEN kills >= 5 THEN 'High Performer'
            WHEN kills >= 2 THEN 'Average'
            WHEN kills >= 1 THEN 'Beginner'
            ELSE 'No Kills'
        END AS combat_performance_tier,
        
        -- Aggression score (normalized 0-100)
        LEAST(100, 
            (attacks_made * 2 + kills * 10 + close_range_engagements * 3)::float / 
            GREATEST(1, duration_minutes) * 10
        ) AS aggression_score,
        
        -- Survival score (normalized 0-100) 
        CASE 
            WHEN duration_minutes > 0 THEN
                LEAST(100, (survival_time_minutes::float / duration_minutes * 100))
            ELSE 0
        END AS survival_score,
        
        -- Item management efficiency
        CASE 
            WHEN items_picked_up > 0 THEN
                (items_picked_up - items_dropped)::float / items_picked_up
            ELSE 0
        END AS item_retention_rate,
        
        -- Positioning score based on zone diversity and center proximity
        (zones_visited * 10 + 
         CASE 
             WHEN avg_distance_from_center < 100000 THEN 20
             WHEN avg_distance_from_center < 200000 THEN 15
             WHEN avg_distance_from_center < 300000 THEN 10
             ELSE 5
         END) AS positioning_score,
        
        -- Overall performance score (weighted composite)
        (
            COALESCE(kills * 10, 0) +
            COALESCE(total_damage / 10, 0) +
            COALESCE(heals_used * 2, 0) +
            COALESCE(zones_visited * 5, 0) +
            CASE 
                WHEN duration_minutes > 0 THEN (survival_time_minutes::float / duration_minutes * 50)
                ELSE 0
            END
        ) AS overall_performance_score
        
    FROM player_match_events
)

SELECT 
    -- Identifiers
    match_id,
    character_account_id,
    player_name,
    platform,
    skill_level,
    map_name,
    game_mode,
    match_date,
    duration_minutes,
    
    -- Basic metrics
    combat_events,
    item_events,
    position_updates,
    
    -- Combat metrics
    kills,
    damage_events_taken,
    attacks_made,
    total_damage,
    avg_damage_per_event,
    kill_to_attack_ratio,
    
    -- Engagement breakdown
    close_range_engagements,
    medium_range_engagements,
    long_range_engagements,
    
    -- Item management
    items_picked_up,
    items_dropped,
    item_retention_rate,
    
    -- Survival and positioning
    survival_time_minutes,
    zones_visited,
    avg_distance_from_center,
    heals_used,
    
    -- Game phase activity
    early_game_events,
    mid_game_events,
    late_game_events,
    
    -- Performance classifications
    activity_level,
    combat_performance_tier,
    
    -- Calculated scores
    aggression_score,
    survival_score,
    positioning_score,
    overall_performance_score,
    
    -- Timestamps
    first_event_time,
    last_event_time,
    
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
    
FROM enriched_summary
WHERE character_account_id IS NOT NULL