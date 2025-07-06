# PUBG API Telemetry Fetcher Usage Guide

This script fetches real telemetry data from the PUBG API and saves it in a format compatible with the DBT project.

## Prerequisites

1. **PUBG API Key**: Get your free API key from [https://developer.pubg.com/](https://developer.pubg.com/)
2. **Python 3.7+**: Make sure Python is installed
3. **Dependencies**: Install required packages

```bash
pip install -r requirements.txt
```

## Basic Usage

### Get API Key
1. Visit [https://developer.pubg.com/](https://developer.pubg.com/)
2. Click "Get your own API key"
3. Fill out the form and submit
4. Copy your API key

### Fetch Telemetry Data

```bash
# Basic usage - fetch data for specific players
python3 pubg_api_fetcher.py --api-key YOUR_API_KEY --players PlayerName1 PlayerName2

# Specify platform (default is steam)
python3 pubg_api_fetcher.py --api-key YOUR_API_KEY --players PlayerName1 --platform xbox

# Limit number of matches per player
python3 pubg_api_fetcher.py --api-key YOUR_API_KEY --players PlayerName1 --max-matches 3

# Custom output directory
python3 pubg_api_fetcher.py --api-key YOUR_API_KEY --players PlayerName1 --output-dir ./my_data
```

## Command Line Options

- `--api-key`: Your PUBG API key (required)
- `--players`: Player names to fetch data for (required, can specify multiple)
- `--platform`: Gaming platform - steam, xbox, or psn (default: steam)
- `--output-dir`: Directory to save files (default: ./pubg_data)
- `--max-matches`: Maximum matches to fetch per player (default: 5)

## How It Works

The script follows the official PUBG API workflow:

1. **Find Players**: Uses player names to get account IDs
2. **Get Matches**: Retrieves recent match IDs for each player
3. **Fetch Match Data**: Gets match metadata including telemetry URLs
4. **Download Telemetry**: Downloads compressed telemetry files and parses events
5. **Save Data**: Outputs JSON files compatible with the DBT project

## Output Files

The script creates three JSON files:

- `players.json` - Player information
- `matches.json` - Match metadata
- `telemetry_events.json` - All telemetry events

## Example Output Structure

### players.json
```json
[
  {
    "account_id": "account.abc123",
    "player_name": "PlayerName1", 
    "platform": "steam",
    "region": "pc-na",
    "ingested_at": "2024-01-15T10:30:00"
  }
]
```

### matches.json
```json
[
  {
    "match_id": "match.def456",
    "map_name": "Erangel",
    "game_mode": "squad", 
    "match_type": "official",
    "created_at": "2024-01-15T09:45:00Z",
    "duration": 1845,
    "telemetry_url": "https://telemetry-cdn.pubg.com/...",
    "ingested_at": "2024-01-15T10:30:00"
  }
]
```

### telemetry_events.json
```json
[
  {
    "_D": "2024-01-15T09:45:12.123Z",
    "_T": "LogPlayerKillV2",
    "common": {"isGame": 1.5},
    "match_id": "match.def456", 
    "event_data": {
      "attackId": 12345,
      "killer": {
        "name": "PlayerName1",
        "accountId": "account.abc123",
        "location": {"x": 123456, "y": 654321, "z": 100}
      },
      "victim": {
        "name": "PlayerName2", 
        "accountId": "account.xyz789",
        "location": {"x": 123400, "y": 654300, "z": 95}
      },
      "distance": 75.2,
      "damage": 120
    },
    "ingested_at": "2024-01-15T10:30:00"
  }
]
```

## Rate Limiting

The script automatically handles PUBG API rate limits:
- Default: 10 requests per minute
- Built-in delays between requests
- Graceful handling of 429 (rate limit) responses

## Error Handling

The script handles common issues:
- Invalid player names
- Network timeouts
- Missing telemetry data
- API rate limits
- Malformed responses

## Integration with DBT Project

The output files are designed to work with the DBT star schema:

1. **Load files into your data warehouse** (Snowflake, BigQuery, etc.)
2. **Update DBT sources** to point to the raw tables
3. **Run DBT models**: `dbt run`
4. **Test data quality**: `dbt test`

## Tips for Better Results

1. **Use exact player names** - case sensitive
2. **Choose the right platform** - make sure players are on the specified platform
3. **Start small** - test with 1-2 players first
4. **Monitor rate limits** - the script waits between requests automatically
5. **Check player activity** - inactive players may have no recent matches

## Troubleshooting

### "No players found"
- Check player names are spelled correctly and case-sensitive
- Verify players are on the specified platform
- Make sure API key is valid

### "Rate limit exceeded"
- Wait a few minutes before retrying
- The script automatically handles rate limiting

### "No matches found"
- Players may not have played recently (API only has 14 days of data)
- Try different players or check if they've been active

### "No telemetry data"
- Some older matches may not have telemetry available
- Try fetching more recent matches

## Example Commands

```bash
# Fetch data for popular streamers (replace with real names)
python3 pubg_api_fetcher.py --api-key YOUR_KEY --players shroud chocoTaco

# Xbox players
python3 pubg_api_fetcher.py --api-key YOUR_KEY --players XboxPlayer1 --platform xbox

# Large dataset with many matches
python3 pubg_api_fetcher.py --api-key YOUR_KEY --players Player1 Player2 Player3 --max-matches 10

# Quick test with minimal data
python3 pubg_api_fetcher.py --api-key YOUR_KEY --players TestPlayer --max-matches 1
```

## Data Freshness

- PUBG API retains match data for **14 days**
- Players must have played recently to have available matches
- Telemetry data is generated for all match types (solo, duo, squad)

## Next Steps

Once you have the data:

1. **Upload to your data warehouse**
2. **Configure DBT sources** in `models/sources.yml`
3. **Run the DBT project**: `dbt run`
4. **Explore your star schema** with the dimensional models
5. **Build dashboards** using the fact and dimension tables

Happy analyzing! 🎮📊