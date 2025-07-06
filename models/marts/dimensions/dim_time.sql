{{
  config(
    materialized='table',
    description='Time dimension table containing temporal attributes'
  )
}}

WITH time_base AS (
    SELECT DISTINCT
        DATE_TRUNC('hour', event_timestamp) AS event_hour,
        event_timestamp
    FROM {{ ref('stg_telemetry_events') }}
    WHERE event_timestamp IS NOT NULL
),

time_expanded AS (
    SELECT 
        event_hour,
        DATE_TRUNC('day', event_hour) AS event_date,
        DATE_TRUNC('week', event_hour) AS event_week,
        DATE_TRUNC('month', event_hour) AS event_month,
        DATE_TRUNC('quarter', event_hour) AS event_quarter,
        DATE_TRUNC('year', event_hour) AS event_year,
        
        -- Extract time components
        EXTRACT(hour FROM event_hour) AS hour_of_day,
        EXTRACT(day FROM event_hour) AS day_of_month,
        EXTRACT(week FROM event_hour) AS week_of_year,
        EXTRACT(month FROM event_hour) AS month_of_year,
        EXTRACT(quarter FROM event_hour) AS quarter_of_year,
        EXTRACT(year FROM event_hour) AS year_number,
        EXTRACT(dayofweek FROM event_hour) AS day_of_week,
        EXTRACT(dayofyear FROM event_hour) AS day_of_year,
        
        -- Day name
        CASE EXTRACT(dayofweek FROM event_hour)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END AS day_name,
        
        -- Month name
        CASE EXTRACT(month FROM event_hour)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        
        -- Quarter name
        CASE EXTRACT(quarter FROM event_hour)
            WHEN 1 THEN 'Q1'
            WHEN 2 THEN 'Q2'
            WHEN 3 THEN 'Q3'
            WHEN 4 THEN 'Q4'
        END AS quarter_name,
        
        -- Season
        CASE 
            WHEN EXTRACT(month FROM event_hour) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(month FROM event_hour) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(month FROM event_hour) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(month FROM event_hour) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        
        -- Time of day
        CASE 
            WHEN EXTRACT(hour FROM event_hour) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(hour FROM event_hour) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(hour FROM event_hour) BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        
        -- Business vs Weekend
        CASE 
            WHEN EXTRACT(dayofweek FROM event_hour) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        
        -- Peak gaming hours
        CASE 
            WHEN EXTRACT(hour FROM event_hour) BETWEEN 18 AND 23 THEN 'Peak'
            WHEN EXTRACT(hour FROM event_hour) BETWEEN 12 AND 17 THEN 'Moderate'
            ELSE 'Off-Peak'
        END AS gaming_period,
        
        -- Work hours flag
        CASE 
            WHEN EXTRACT(hour FROM event_hour) BETWEEN 9 AND 17 
                 AND EXTRACT(dayofweek FROM event_hour) NOT IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_work_hours,
        
        -- Weekend flag
        CASE 
            WHEN EXTRACT(dayofweek FROM event_hour) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        
        -- Holiday flag (basic implementation)
        CASE 
            WHEN EXTRACT(month FROM event_hour) = 12 AND EXTRACT(day FROM event_hour) = 25 THEN TRUE
            WHEN EXTRACT(month FROM event_hour) = 1 AND EXTRACT(day FROM event_hour) = 1 THEN TRUE
            WHEN EXTRACT(month FROM event_hour) = 7 AND EXTRACT(day FROM event_hour) = 4 THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        
        -- First/last day of month
        CASE 
            WHEN EXTRACT(day FROM event_hour) = 1 THEN TRUE
            ELSE FALSE
        END AS is_first_day_of_month,
        
        CASE 
            WHEN event_hour = DATE_TRUNC('month', event_hour) + INTERVAL '1 month' - INTERVAL '1 day' THEN TRUE
            ELSE FALSE
        END AS is_last_day_of_month,
        
        -- Gaming activity level by hour
        CASE 
            WHEN EXTRACT(hour FROM event_hour) IN (20, 21, 22) THEN 'Very High'
            WHEN EXTRACT(hour FROM event_hour) IN (19, 23) THEN 'High'
            WHEN EXTRACT(hour FROM event_hour) IN (15, 16, 17, 18) THEN 'Medium'
            WHEN EXTRACT(hour FROM event_hour) IN (12, 13, 14) THEN 'Low'
            ELSE 'Very Low'
        END AS expected_activity_level
    FROM time_base
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['event_hour']) }} AS time_key,
        event_hour,
        event_date,
        event_week,
        event_month,
        event_quarter,
        event_year,
        hour_of_day,
        day_of_month,
        week_of_year,
        month_of_year,
        quarter_of_year,
        year_number,
        day_of_week,
        day_of_year,
        day_name,
        month_name,
        quarter_name,
        season,
        time_of_day,
        day_type,
        gaming_period,
        is_work_hours,
        is_weekend,
        is_holiday,
        is_first_day_of_month,
        is_last_day_of_month,
        expected_activity_level,
        
        -- Formatted date strings
        TO_CHAR(event_date, 'YYYY-MM-DD') AS date_string,
        TO_CHAR(event_hour, 'YYYY-MM-DD HH24:00:00') AS hour_string,
        TO_CHAR(event_week, 'YYYY-"W"WW') AS week_string,
        TO_CHAR(event_month, 'YYYY-MM') AS month_string,
        TO_CHAR(event_quarter, 'YYYY-"Q"Q') AS quarter_string,
        TO_CHAR(event_year, 'YYYY') AS year_string,
        
        -- Days since epoch
        DATEDIFF(day, '1970-01-01', event_date) AS days_since_epoch,
        
        -- Relative time flags
        CASE 
            WHEN event_date = CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_today,
        
        CASE 
            WHEN event_date = CURRENT_DATE - 1 THEN TRUE
            ELSE FALSE
        END AS is_yesterday,
        
        CASE 
            WHEN event_date >= CURRENT_DATE - 7 THEN TRUE
            ELSE FALSE
        END AS is_last_7_days,
        
        CASE 
            WHEN event_date >= CURRENT_DATE - 30 THEN TRUE
            ELSE FALSE
        END AS is_last_30_days,
        
        -- Record metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM time_expanded
)

SELECT * FROM final
ORDER BY event_hour