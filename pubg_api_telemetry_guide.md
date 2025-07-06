# PUBG API Telemetry Objects and Events Guide

## Overview

The PUBG API provides comprehensive telemetry data that contains detailed information about every event that occurs during a match. This includes player movements, weapon usage, item interactions, damage events, and much more.

## Key Resources

- **Official Documentation**: https://documentation.pubg.com/en/telemetry.html
- **Telemetry Events**: https://documentation.pubg.com/en/telemetry-events.html
- **Telemetry Objects**: https://documentation.pubg.com/en/telemetry-objects.html
- **API Assets & Data Dictionaries**: https://github.com/pubg/api-assets

## How to Access Telemetry Data

### Step 1: Get a Match Object
First, you need to retrieve a match from the matches endpoint. You'll need a match ID, which can be found in player objects from the players endpoint.

```bash
curl "https://api.pubg.com/shards/$platform/matches/$matchId" \
-H "Accept: application/vnd.api+json" \
-H "Authorization: Bearer $API_KEY"
```

### Step 2: Find the Telemetry Asset
Look for the "assets" reference in the match "relationships" object:

```json
"relationships": {
  "assets": {
    "data": [
      {
        "type": "asset",
        "id": "1ad97f85-cf9b-11e7-b84e-0a586460f004"
      }
    ]
  }
}
```

### Step 3: Get the Telemetry URL
Find the telemetry object with the matching ID in the included array:

```json
{
  "type": "asset",
  "id": "1ad97f85-cf9b-11e7-b84e-0a586460f004",
  "attributes": {
    "URL": "https://telemetry-cdn.pubg.com/pc-krjp/2018/01/01/0/0/1ad97f85-cf9b-11e7-b84e-0a586460f004-telemetry.json",
    "createdAt": "2018-01-01T00:00:00Z",
    "description": "",
    "name": "telemetry"
  }
}
```

### Step 4: Download Telemetry Data
Download the telemetry file (no API key required):

```bash
curl --compressed "https://telemetry-cdn.pubg.com/pc-krjp/2018/01/01/0/0/1ad97f85-cf9b-11e7-b84e-0a586460f004-telemetry.json" \
-H "Accept: application/vnd.api+json" \
-H "Accept-Encoding: gzip"
```

## Telemetry Events

Each telemetry event contains these common fields:
- `"_D"`: Event timestamp
- `"_T"`: Event type  
- `"common"`: Common object with game state info

### Key Event Types

#### Player Actions
- **LogPlayerAttack**: Player attack events
- **LogPlayerKillV2**: Player kill events (current version)
- **LogPlayerTakeDamage**: Damage taken by players
- **LogPlayerMakeGroggy**: Player knocked down events
- **LogPlayerRevive**: Player revival events
- **LogPlayerPosition**: Player position tracking

#### Item Management
- **LogItemPickup**: Item pickup events
- **LogItemDrop**: Item drop events
- **LogItemEquip**: Item equip events
- **LogItemUse**: Item usage events
- **LogItemAttach**: Item attachment events
- **LogItemPickupFromCarepackage**: Care package item pickups

#### Vehicle Events
- **LogVehicleRide**: Vehicle boarding events
- **LogVehicleLeave**: Vehicle exit events
- **LogVehicleDamage**: Vehicle damage events
- **LogVehicleDestroy**: Vehicle destruction events

#### Match Events
- **LogMatchStart**: Match beginning
- **LogMatchEnd**: Match conclusion
- **LogPhaseChange**: Blue zone phase changes
- **LogCarePackageLand**: Care package drops

#### Healing & Support
- **LogHeal**: Healing events
- **LogPlayerCreate**: Player spawn events

## Telemetry Objects

### Character Object
```json
{
  "name": "string",
  "teamId": "int",
  "health": "number",
  "location": {"Location"},
  "ranking": "int",
  "accountId": "string",
  "isInBlueZone": "bool",
  "isInRedZone": "bool",
  "zone": ["regionId", "..."]
}
```

### Location Object
```json
{
  "x": "number",
  "y": "number", 
  "z": "number"
}
```

**Important Notes:**
- Location values are in centimeters
- (0,0) is at the top-left of each map
- Different maps have different coordinate ranges:
  - Erangel, Miramar, Taego, Vikendi, Deston: 0-816,000
  - Sanhok: 0-408,000
  - Paramo: 0-306,000
  - Karakin, Range: 0-204,000
  - Haven: 0-102,000

### Item Object
```json
{
  "itemId": "string",
  "stackCount": "int",
  "category": "string",
  "subCategory": "string",
  "attachedItems": ["itemId", "..."]
}
```

### Vehicle Object
```json
{
  "vehicleType": "string",
  "vehicleId": "string",
  "vehicleUniqueId": "int",
  "healthPercent": "number",
  "feulPercent": "number",
  "altitudeAbs": "number",
  "altitudeRel": "number",
  "velocity": "number",
  "seatIndex": "int",
  "isWheelsInAir": "bool",
  "isInWaterVolume": "bool",
  "isEngineOn": "bool"
}
```

### GameState Object
```json
{
  "elapsedTime": "int",
  "numAliveTeams": "int",
  "numJoinPlayers": "int",
  "numStartPlayers": "int",
  "numAlivePlayers": "int",
  "safetyZonePosition": {"Location"},
  "safetyZoneRadius": "number",
  "poisonGasWarningPosition": {"Location"},
  "poisonGasWarningRadius": "number",
  "redZonePosition": {"Location"},
  "redZoneRadius": "number",
  "blackZonePosition": {"Location"},
  "blackZoneRadius": "number"
}
```

## Data Dictionaries & Enums

The official PUBG API Assets repository contains:

### Assets Folder
- Equipment and weapon images
- HUD icons
- Map assets
- Organized by telemetry object categories

### Dictionaries Folder
- Maps technical names to human-readable names
- Example: `"Item_Attach_Weapon_Stock_SniperRifle_CheekPad_C"` → `"Sniper Rifle Cheek Pad"`

### Enums Folder
- Lists of possible values for telemetry fields
- Categories, subcategories, damage types, etc.

## API Requirements

### Authentication
- API Key required for match/player data
- No API Key required for telemetry file download

### Rate Limits
- Default: 10 requests per minute
- Higher limits available upon request

### Data Retention
- 14 days of match data retention
- Telemetry data compressed using gzip

## Example Use Cases

### Player Tracking
- Track player movements throughout a match
- Create heatmaps of player positions
- Analyze player behavior patterns

### Combat Analysis
- Analyze weapon usage statistics
- Track damage patterns and kill events
- Study engagement ranges and outcomes

### Match Visualization
- Create full match replays
- Visualize blue zone movements
- Track care package and item distributions

### Performance Analytics
- Calculate player performance metrics
- Analyze survival strategies
- Study team coordination patterns

## Getting Started

1. **Get API Key**: Visit https://developer.pubg.com/ to request access
2. **Review Documentation**: Read the full API documentation
3. **Join Community**: Join the PUBG Developer Discord for support
4. **Start Small**: Begin with player endpoint, then move to telemetry
5. **Use Resources**: Leverage the GitHub assets for data dictionaries

## Important Considerations

- **Data Volume**: Telemetry files can be large (several MB per match)
- **Processing Time**: Parsing telemetry data can be time-consuming
- **Compression**: Always specify gzip encoding for better performance
- **Rate Limits**: Plan your requests to stay within API limits
- **Data Retention**: Cache important data as it's only available for 14 days

## Community Resources

- **Official Forum**: PUBG Developer Forum
- **Discord**: PUBG Developer API Discord server
- **GitHub**: Official API assets and community contributions
- **Featured Apps**: Examples of successful PUBG API applications

This telemetry system provides incredibly detailed match data that enables developers to create sophisticated analysis tools, visualizations, and applications for the PUBG community.