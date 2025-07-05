{{
  config(
    materialized='table',
    description='Location dimension table containing spatial attributes and zones'
  )
}}

WITH location_base AS (
    SELECT DISTINCT
        location_x,
        location_y,
        location_z,
        match_id
    FROM {{ ref('stg_telemetry_events') }}
    WHERE location_x IS NOT NULL 
      AND location_y IS NOT NULL 
      AND location_z IS NOT NULL
),

location_with_zones AS (
    SELECT
        location_x,
        location_y,
        location_z,
        match_id,
        
        -- Map-based zone classification (using coordinate ranges from documentation)
        CASE 
            -- Erangel zones (0-816,000 range)
            WHEN location_x BETWEEN 0 AND 816000 AND location_y BETWEEN 0 AND 816000 THEN
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
        END AS map_zone,
        
        -- Distance from center calculation
        SQRT(
            POWER(location_x - 408000, 2) + 
            POWER(location_y - 408000, 2)
        ) AS distance_from_center,
        
        -- Elevation category
        CASE 
            WHEN location_z >= 1000 THEN 'High'
            WHEN location_z >= 500 THEN 'Medium'
            WHEN location_z >= 100 THEN 'Low'
            ELSE 'Ground'
        END AS elevation_category,
        
        -- Quadrant classification
        CASE 
            WHEN location_x <= 408000 AND location_y <= 408000 THEN 'Q1'
            WHEN location_x > 408000 AND location_y <= 408000 THEN 'Q2'
            WHEN location_x <= 408000 AND location_y > 408000 THEN 'Q3'
            WHEN location_x > 408000 AND location_y > 408000 THEN 'Q4'
            ELSE 'Unknown'
        END AS quadrant
    FROM location_base
),

location_stats AS (
    SELECT 
        location_x,
        location_y,
        location_z,
        COUNT(*) AS event_count,
        COUNT(DISTINCT match_id) AS match_count,
        AVG(distance_from_center) AS avg_distance_from_center
    FROM location_with_zones
    GROUP BY location_x, location_y, location_z
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['l.location_x', 'l.location_y', 'l.location_z']) }} AS location_key,
        l.location_x,
        l.location_y,
        l.location_z,
        l.map_zone,
        l.distance_from_center,
        l.elevation_category,
        l.quadrant,
        
        -- Location statistics
        COALESCE(s.event_count, 0) AS event_count,
        COALESCE(s.match_count, 0) AS match_count,
        
        -- Location popularity
        CASE 
            WHEN s.event_count >= 1000 THEN 'Hotspot'
            WHEN s.event_count >= 500 THEN 'Popular'
            WHEN s.event_count >= 100 THEN 'Moderate'
            WHEN s.event_count >= 10 THEN 'Occasional'
            ELSE 'Rare'
        END AS location_popularity,
        
        -- Distance from center categories
        CASE 
            WHEN l.distance_from_center <= 100000 THEN 'Center'
            WHEN l.distance_from_center <= 200000 THEN 'Inner'
            WHEN l.distance_from_center <= 300000 THEN 'Mid'
            WHEN l.distance_from_center <= 400000 THEN 'Outer'
            ELSE 'Edge'
        END AS distance_category,
        
        -- Coordinate grid system (simplified grid)
        CONCAT(
            CHAR(65 + FLOOR(l.location_x / 102000)), -- A, B, C, etc.
            CAST(FLOOR(l.location_y / 102000) + 1 AS VARCHAR) -- 1, 2, 3, etc.
        ) AS grid_reference,
        
        -- Location type based on coordinates
        CASE 
            WHEN l.location_x <= 50000 OR l.location_x >= 766000 OR 
                 l.location_y <= 50000 OR l.location_y >= 766000 THEN 'Edge'
            WHEN l.location_x <= 150000 OR l.location_x >= 666000 OR 
                 l.location_y <= 150000 OR l.location_y >= 666000 THEN 'Border'
            WHEN l.distance_from_center <= 150000 THEN 'Central'
            ELSE 'Interior'
        END AS location_type,
        
        -- Safe zone likelihood (closer to center = more likely)
        CASE 
            WHEN l.distance_from_center <= 100000 THEN 'High'
            WHEN l.distance_from_center <= 200000 THEN 'Medium'
            WHEN l.distance_from_center <= 300000 THEN 'Low'
            ELSE 'Very Low'
        END AS safe_zone_likelihood,
        
        -- Normalized coordinates (0-1 range)
        l.location_x / 816000.0 AS normalized_x,
        l.location_y / 816000.0 AS normalized_y,
        
        -- Is corner location
        CASE 
            WHEN (l.location_x <= 100000 AND l.location_y <= 100000) OR
                 (l.location_x <= 100000 AND l.location_y >= 716000) OR
                 (l.location_x >= 716000 AND l.location_y <= 100000) OR
                 (l.location_x >= 716000 AND l.location_y >= 716000) THEN TRUE
            ELSE FALSE
        END AS is_corner,
        
        -- Is edge location
        CASE 
            WHEN l.location_x <= 50000 OR l.location_x >= 766000 OR 
                 l.location_y <= 50000 OR l.location_y >= 766000 THEN TRUE
            ELSE FALSE
        END AS is_edge,
        
        -- Record metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM location_with_zones l
    LEFT JOIN location_stats s ON l.location_x = s.location_x 
                                AND l.location_y = s.location_y 
                                AND l.location_z = s.location_z
)

SELECT DISTINCT * FROM final