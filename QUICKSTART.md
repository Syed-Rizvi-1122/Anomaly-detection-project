# 🚀 PUBG Telemetry DBT Quick Start Guide

This guide will help you quickly generate sample PUBG telemetry data and run the DBT project.

## Prerequisites

- Python 3.7+ installed
- DBT Core installed (`pip install dbt-core dbt-snowflake` or your data warehouse adapter)
- For S3 upload: AWS CLI configured or AWS credentials

## 🎮 Step 1: Generate Sample Data

You have two options for generating data:

### Option A: Generate Data Locally (Recommended for Testing)

```bash
# Generate small dataset for testing
./scripts/run_data_generation.sh local

# Or customize the dataset size
./scripts/run_data_generation.sh local -m 10 -p 100 -e 500

# Generate larger dataset
./scripts/run_data_generation.sh local -m 20 -p 200 -e 1000 -f jsonl
```

This creates files in `./sample_data/`:
- `players.json` - Player information
- `matches.json` - Match metadata  
- `telemetry_events.json` - Telemetry events

### Option B: Generate and Upload to S3

```bash
# Set up AWS credentials first
aws configure

# Generate and upload to S3
./scripts/run_data_generation.sh s3 -b my-pubg-bucket

# Larger dataset with custom settings
./scripts/run_data_generation.sh s3 -b my-bucket -m 50 -p 500 -e 2000 --prefix "pubg-data/"
```

## 🏗️ Step 2: Set Up Your Data Warehouse

### For Snowflake

1. Create database and schemas:

```sql
-- Create database
CREATE DATABASE PUBG_DW;

-- Create schemas
CREATE SCHEMA PUBG_DW.RAW_PUBG;
CREATE SCHEMA PUBG_DW.STAGING;
CREATE SCHEMA PUBG_DW.MARTS;
```

2. Load the generated JSON data into raw tables:

```sql
-- Create raw tables
CREATE TABLE PUBG_DW.RAW_PUBG.TELEMETRY_EVENTS (
    _D TIMESTAMP,
    _T VARCHAR,
    COMMON VARIANT,
    MATCH_ID VARCHAR,
    EVENT_DATA VARIANT,
    INGESTED_AT TIMESTAMP
);

CREATE TABLE PUBG_DW.RAW_PUBG.MATCHES (
    MATCH_ID VARCHAR,
    MAP_NAME VARCHAR,
    GAME_MODE VARCHAR,
    MATCH_TYPE VARCHAR,
    CREATED_AT TIMESTAMP,
    DURATION INTEGER,
    TELEMETRY_URL VARCHAR,
    INGESTED_AT TIMESTAMP
);

CREATE TABLE PUBG_DW.RAW_PUBG.PLAYERS (
    ACCOUNT_ID VARCHAR,
    PLAYER_NAME VARCHAR,
    PLATFORM VARCHAR,
    REGION VARCHAR,
    INGESTED_AT TIMESTAMP
);

-- Load data from files (adjust file paths as needed)
COPY INTO PUBG_DW.RAW_PUBG.TELEMETRY_EVENTS
FROM @my_stage/telemetry_events.json
FILE_FORMAT = (TYPE = JSON);

COPY INTO PUBG_DW.RAW_PUBG.MATCHES  
FROM @my_stage/matches.json
FILE_FORMAT = (TYPE = JSON);

COPY INTO PUBG_DW.RAW_PUBG.PLAYERS
FROM @my_stage/players.json
FILE_FORMAT = (TYPE = JSON);
```

### For BigQuery

```sql
-- Create dataset
CREATE SCHEMA `your-project.pubg_raw`;

-- Load JSON files using the BigQuery console or bq CLI
bq load --source_format=NEWLINE_DELIMITED_JSON \
  your-project:pubg_raw.telemetry_events \
  ./sample_data/telemetry_events.jsonl

bq load --source_format=NEWLINE_DELIMITED_JSON \
  your-project:pubg_raw.matches \
  ./sample_data/matches.jsonl
  
bq load --source_format=NEWLINE_DELIMITED_JSON \
  your-project:pubg_raw.players \
  ./sample_data/players.jsonl
```

## 🔧 Step 3: Configure DBT

1. Set up your `profiles.yml` file:

```yaml
# ~/.dbt/profiles.yml
pubg_telemetry_dw:
  target: dev
  outputs:
    dev:
      type: snowflake  # or bigquery, redshift, etc.
      account: your_account
      user: your_username
      password: your_password
      role: your_role
      database: PUBG_DW
      warehouse: COMPUTE_WH
      schema: DEV
      threads: 4
```

2. Install DBT packages:

```bash
dbt deps
```

3. Test the connection:

```bash
dbt debug
```

## 🎯 Step 4: Run the DBT Project

1. Run all models:

```bash
dbt run
```

2. Run tests:

```bash
dbt test
```

3. Generate documentation:

```bash
dbt docs generate
dbt docs serve
```

## 📊 Step 5: Explore Your Data

Once the models are built, you can query the star schema:

### Sample Queries

**Player Performance Summary:**
```sql
SELECT 
    player_name,
    platform,
    skill_level,
    total_matches,
    avg_kills_per_match,
    activity_level
FROM marts.dim_players
WHERE total_matches > 5
ORDER BY avg_kills_per_match DESC
LIMIT 10;
```

**Combat Hotspots by Map:**
```sql
SELECT 
    m.map_name,
    l.map_zone,
    COUNT(*) as combat_events,
    AVG(c.damage) as avg_damage
FROM facts.fact_combat_events c
JOIN dimensions.dim_matches m ON c.match_key = m.match_key
JOIN dimensions.dim_locations l ON c.location_key = l.location_key
WHERE c.combat_action = 'Kill'
GROUP BY m.map_name, l.map_zone
ORDER BY combat_events DESC;
```

**Item Popularity Trends:**
```sql
SELECT 
    i.clean_item_name,
    i.item_type,
    COUNT(*) as pickup_count,
    i.popularity_tier
FROM facts.fact_item_events f
JOIN dimensions.dim_items i ON f.item_key = i.item_key
WHERE f.item_action = 'Pickup'
GROUP BY i.clean_item_name, i.item_type, i.popularity_tier
ORDER BY pickup_count DESC
LIMIT 20;
```

**Match Performance Analysis:**
```sql
SELECT 
    player_name,
    map_name,
    game_mode,
    kills,
    survival_score,
    overall_performance_score,
    combat_performance_tier
FROM core.player_match_summary
WHERE overall_performance_score > 100
ORDER BY overall_performance_score DESC;
```

## 🔄 Step 6: Incremental Loading (Optional)

For production use, set up incremental loading:

1. Modify staging models to use incremental materialization:

```sql
{{
  config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail'
  )
}}

SELECT * FROM {{ source('pubg_raw', 'telemetry_events') }}

{% if is_incremental() %}
    WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
```

2. Schedule DBT runs:

```bash
# Run only new/changed models
dbt run --select state:modified+

# Run incrementally
dbt run --vars '{"start_date": "2024-01-01"}'
```

## 🎨 Step 7: Create Dashboards

Connect your BI tool to the dimensional tables:

- **Fact Tables**: `facts.fact_telemetry_events`, `facts.fact_combat_events`, `facts.fact_item_events`
- **Dimension Tables**: `dimensions.dim_players`, `dimensions.dim_matches`, `dimensions.dim_items`, `dimensions.dim_locations`, `dimensions.dim_time`
- **Summary Tables**: `core.player_match_summary`

## 🆘 Troubleshooting

### Common Issues

**DBT connection errors:**
```bash
dbt debug  # Check connection
```

**Python dependencies:**
```bash
pip install -r scripts/requirements.txt
```

**AWS credentials for S3:**
```bash
aws configure
# or set environment variables:
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

**Large dataset performance:**
- Use `--threads` parameter for parallel execution
- Consider clustering/partitioning on `event_timestamp` and `match_id`
- Use incremental models for large fact tables

### Scaling Up

For production workloads:

1. **Increase data volume:**
```bash
./scripts/run_data_generation.sh s3 -b prod-bucket -m 1000 -p 10000 -e 5000
```

2. **Optimize DBT performance:**
```bash
dbt run --threads 8
```

3. **Set up orchestration:**
- Use Airflow, Prefect, or dbt Cloud for scheduling
- Set up data freshness tests
- Monitor model performance

## 🎯 Next Steps

1. **Customize the schema** for your specific analytics needs
2. **Add more event types** by extending the telemetry parsing logic
3. **Create ML features** using the dimensional data
4. **Set up alerts** for data quality issues
5. **Build real-time dashboards** for match analytics

## 📚 Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [PUBG API Documentation](https://documentation.pubg.com/)
- [Star Schema Design Patterns](https://www.kimballgroup.com/)
- [Dimensional Modeling Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)

Happy analyzing! 🎮📊