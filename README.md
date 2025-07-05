# PUBG Telemetry Data Warehouse - DBT Project

This DBT project creates a comprehensive data warehouse for PUBG telemetry data using a star schema design. The project transforms raw telemetry events into dimensional models optimized for analytics and reporting.

## Project Overview

### Architecture
- **Raw Layer**: Raw PUBG telemetry data from API ingestion
- **Staging Layer**: Cleaned and standardized data models
- **Dimensional Layer**: Star schema with fact and dimension tables
- **Mart Layer**: Business-ready tables for analytics

### Star Schema Design

#### Fact Tables
- `fact_telemetry_events` - Main fact table containing all telemetry events
- `fact_combat_events` - Specialized fact table for combat-related events
- `fact_item_events` - Specialized fact table for item-related events

#### Dimension Tables
- `dim_players` - Player information and statistics
- `dim_matches` - Match metadata and characteristics
- `dim_items` - Item catalog with usage statistics
- `dim_locations` - Spatial coordinates and zone information
- `dim_time` - Time dimension with temporal attributes

## Data Sources

The project expects the following raw data sources in the `raw_pubg` schema:

- `telemetry_events` - Raw telemetry events from PUBG API
- `matches` - Match metadata
- `players` - Player information

## Key Features

### Telemetry Event Processing
- Comprehensive parsing of all PUBG telemetry event types
- Support for 40+ different event types including:
  - Combat events (kills, damage, attacks)
  - Item events (pickup, drop, equip, use)
  - Vehicle events (ride, leave, damage)
  - Match events (start, end, phase changes)

### Advanced Analytics Features
- **Spatial Analytics**: Location-based analysis with map zones and distance calculations
- **Combat Analytics**: Engagement range analysis, damage categorization, combat intensity scoring
- **Item Analytics**: Item popularity, usage patterns, acquisition sources
- **Temporal Analytics**: Time-based patterns, game phases, peak hours analysis

### Data Quality & Testing
- Comprehensive data quality checks
- Missing data flagging
- Referential integrity tests
- Business rule validations

## Quick Start

### Prerequisites
- DBT Core 1.0+
- Snowflake/BigQuery/Redshift connection
- Raw PUBG telemetry data

### Setup
1. Clone this repository
2. Configure your `profiles.yml` file (see example below)
3. Install dependencies: `dbt deps`
4. Run the project: `dbt run`
5. Test the models: `dbt test`
6. Generate documentation: `dbt docs generate && dbt docs serve`

### Sample Profile Configuration

```yaml
pubg_telemetry_dw:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_username
      password: your_password
      role: your_role
      database: PUBG_DW
      warehouse: COMPUTE_WH
      schema: DEV
      threads: 4
      client_session_keep_alive: False
```

## Model Descriptions

### Staging Models
- **stg_telemetry_events**: Cleaned and parsed telemetry events with extracted attributes
- **stg_matches**: Standardized match metadata with derived attributes
- **stg_players**: Cleaned player information with platform standardization

### Dimension Models
- **dim_players**: Enhanced player profiles with activity metrics and skill classifications
- **dim_matches**: Rich match metadata with intensity metrics and temporal attributes
- **dim_items**: Item catalog with popularity rankings and usage analytics
- **dim_locations**: Spatial dimension with zone mappings and distance calculations
- **dim_time**: Comprehensive time dimension with gaming-specific attributes

### Fact Models
- **fact_telemetry_events**: Central fact table linking all events to dimensions
- **fact_combat_events**: Combat-focused fact table with engagement analytics
- **fact_item_events**: Item-focused fact table with inventory management analytics

## Analytics Use Cases

### Player Performance Analysis
```sql
SELECT 
    p.player_name,
    p.skill_level,
    COUNT(f.event_id) as total_events,
    SUM(f.is_kill_event) as total_kills,
    AVG(f.damage) as avg_damage
FROM fact_telemetry_events f
JOIN dim_players p ON f.player_key = p.player_key
WHERE f.event_category = 'Combat'
GROUP BY p.player_name, p.skill_level
ORDER BY total_kills DESC;
```

### Map Hotspot Analysis
```sql
SELECT 
    l.map_zone,
    l.location_popularity,
    COUNT(*) as event_count,
    COUNT(DISTINCT f.match_id) as matches_with_activity
FROM fact_telemetry_events f
JOIN dim_locations l ON f.location_key = l.location_key
GROUP BY l.map_zone, l.location_popularity
ORDER BY event_count DESC;
```

### Item Popularity Trends
```sql
SELECT 
    i.clean_item_name,
    i.item_type,
    i.popularity_tier,
    t.day_name,
    COUNT(*) as pickup_count
FROM fact_item_events f
JOIN dim_items i ON f.item_key = i.item_key
JOIN dim_time t ON f.time_key = t.time_key
WHERE f.item_action = 'Pickup'
GROUP BY i.clean_item_name, i.item_type, i.popularity_tier, t.day_name
ORDER BY pickup_count DESC;
```

## Performance Considerations

### Incremental Loading
The project is designed for incremental loading:
- Use `event_timestamp` for incremental strategies
- Consider partitioning large fact tables by date
- Implement soft deletes for dimension tables

### Optimization Tips
- Create appropriate indexes on foreign keys
- Consider clustering on frequently filtered columns
- Use appropriate materialization strategies per environment

## Data Dictionary

### Common Telemetry Fields
- **event_timestamp**: When the event occurred in the game
- **event_type**: Type of telemetry event (40+ different types supported)
- **match_id**: Unique identifier for the match
- **character_account_id**: Player's account identifier
- **location_x/y/z**: Spatial coordinates in centimeters
- **is_game**: Game phase indicator (0=pre-game, 1.0=early game, 2.0+=late game)

### Location Coordinate System
- Origin (0,0) is at the top-left corner of maps
- Coordinates are in centimeters
- Different maps have different ranges:
  - Large maps (Erangel, Miramar): 0-816,000
  - Medium maps (Sanhok): 0-408,000
  - Small maps (Karakin): 0-204,000

### Combat Event Details
- **damage**: Actual damage dealt (after armor reduction)
- **distance**: Distance between attacker and victim
- **attack_id**: Links related combat events together

## Contributing

1. Follow DBT best practices for model organization
2. Add appropriate tests for new models
3. Update documentation for schema changes
4. Use semantic versioning for releases

## Support

For questions about PUBG API or telemetry data:
- PUBG API Documentation: https://documentation.pubg.com/
- PUBG Developer Discord: [Community Discord]
- GitHub Issues: [Project Issues]

## License

This project is provided under the MIT License. PUBG and PLAYERUNKNOWN'S BATTLEGROUNDS are trademarks of KRAFTON, Inc.