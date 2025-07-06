#!/usr/bin/env python3
"""
PUBG API Telemetry Fetcher

This script fetches real telemetry data from the PUBG API by:
1. Finding players by name
2. Getting their recent matches
3. Downloading telemetry data from matches
4. Saving data in format compatible with DBT project

Requires PUBG API key from: https://developer.pubg.com/
"""

import requests
import json
import gzip
import time
import argparse
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import sys


class PUBGAPIClient:
    """Client for interacting with PUBG API"""
    
    BASE_URL = "https://api.pubg.com"
    
    # Platform shards from PUBG API documentation
    PLATFORMS = {
        'steam': 'steam',
        'xbox': 'xbox', 
        'psn': 'psn',
        'tournament': 'tournament'
    }
    
    # Platform-region shards
    PLATFORM_REGIONS = {
        'pc-as': 'pc-as',      # Asia
        'pc-eu': 'pc-eu',      # Europe  
        'pc-jp': 'pc-jp',      # Japan
        'pc-kakao': 'pc-kakao', # Kakao
        'pc-krjp': 'pc-krjp',  # Korea
        'pc-na': 'pc-na',      # North America
        'pc-oc': 'pc-oc',      # Oceania
        'pc-ru': 'pc-ru',      # Russia
        'pc-sa': 'pc-sa',      # South America
        'pc-sea': 'pc-sea',    # South East Asia
        'xbox-as': 'xbox-as',
        'xbox-eu': 'xbox-eu', 
        'xbox-na': 'xbox-na',
        'xbox-oc': 'xbox-oc',
        'xbox-sa': 'xbox-sa',
        'psn-as': 'psn-as',
        'psn-eu': 'psn-eu',
        'psn-na': 'psn-na', 
        'psn-oc': 'psn-oc'
    }
    
    def __init__(self, api_key: str, platform: str = 'steam'):
        self.api_key = api_key
        self.platform = platform
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Accept': 'application/vnd.api+json'
        })
        
        # Rate limiting - PUBG API default is 10 requests per minute
        self.last_request_time = 0
        self.min_request_interval = 6  # seconds between requests
        
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            print(f"⏰ Rate limiting: waiting {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _make_request(self, url: str) -> Optional[Dict[str, Any]]:
        """Make API request with rate limiting and error handling"""
        self._wait_for_rate_limit()
        
        try:
            print(f"🌐 Making request to: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"❌ Rate limit exceeded. Wait before retrying.")
                return None
            elif response.status_code == 404:
                print(f"❌ Resource not found: {url}")
                return None
            else:
                print(f"❌ HTTP Error {response.status_code}: {e}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            return None
    
    def get_players_by_names(self, player_names: List[str]) -> List[Dict[str, Any]]:
        """Get player data by player names"""
        if not player_names:
            return []
            
        # PUBG API allows comma-separated player names
        names_param = ','.join(player_names)
        url = f"{self.BASE_URL}/shards/{self.platform}/players?filter[playerNames]={names_param}"
        
        response_data = self._make_request(url)
        if not response_data or 'data' not in response_data:
            return []
            
        players = []
        for player_data in response_data['data']:
            player = {
                'account_id': player_data['id'],
                'player_name': player_data['attributes']['name'],
                'platform': self.platform,
                'region': player_data['attributes'].get('shardId', ''),
                'ingested_at': datetime.now().isoformat()
            }
            players.append(player)
            
        print(f"✅ Found {len(players)} players")
        return players
    
    def get_player_matches(self, player_data: Dict[str, Any]) -> List[str]:
        """Get recent match IDs for a player"""
        account_id = player_data['account_id']
        url = f"{self.BASE_URL}/shards/{self.platform}/players/{account_id}"
        
        response_data = self._make_request(url)
        if not response_data or 'data' not in response_data:
            return []
            
        # Extract match IDs from relationships
        relationships = response_data['data'].get('relationships', {})
        matches = relationships.get('matches', {}).get('data', [])
        
        match_ids = [match['id'] for match in matches]
        print(f"✅ Found {len(match_ids)} matches for {player_data['player_name']}")
        return match_ids
    
    def get_match_data(self, match_id: str) -> Optional[Dict[str, Any]]:
        """Get match data including telemetry URL"""
        url = f"{self.BASE_URL}/shards/{self.platform}/matches/{match_id}"
        
        response_data = self._make_request(url)
        if not response_data or 'data' not in response_data:
            return None
            
        match_data = response_data['data']
        attributes = match_data.get('attributes', {})
        
        # Extract basic match info
        match_info = {
            'match_id': match_id,
            'map_name': attributes.get('mapName', ''),
            'game_mode': attributes.get('gameMode', ''),
            'match_type': attributes.get('matchType', ''),
            'created_at': attributes.get('createdAt', ''),
            'duration': attributes.get('duration', 0),
            'ingested_at': datetime.now().isoformat()
        }
        
        # Find telemetry URL in included assets
        telemetry_url = None
        included = response_data.get('included', [])
        
        # Look for telemetry asset
        for item in included:
            if item.get('type') == 'asset' and item.get('attributes', {}).get('name') == 'telemetry':
                telemetry_url = item.get('attributes', {}).get('URL')
                break
                
        match_info['telemetry_url'] = telemetry_url
        
        print(f"✅ Retrieved match data for {match_id}")
        return match_info
    
    def download_telemetry_data(self, telemetry_url: str, match_id: str) -> List[Dict[str, Any]]:
        """Download and parse telemetry data from URL"""
        if not telemetry_url:
            print(f"❌ No telemetry URL for match {match_id}")
            return []
            
        try:
            print(f"📥 Downloading telemetry data from: {telemetry_url}")
            
            # Download telemetry file (no API key needed)
            headers = {
                'Accept-Encoding': 'gzip',
                'Accept': 'application/json'
            }
            
            response = requests.get(telemetry_url, headers=headers)
            response.raise_for_status()
            
            # Handle gzip compression
            if response.headers.get('content-encoding') == 'gzip':
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            # Parse JSON data
            telemetry_events = json.loads(content)
            
            # Add match_id and ingested_at to each event
            processed_events = []
            for event in telemetry_events:
                event['match_id'] = match_id
                event['ingested_at'] = datetime.now().isoformat()
                
                # Restructure to match DBT schema
                processed_event = {
                    '_D': event.get('_D'),
                    '_T': event.get('_T'), 
                    'common': event.get('common', {}),
                    'match_id': match_id,
                    'event_data': {k: v for k, v in event.items() 
                                 if k not in ['_D', '_T', 'common', 'match_id', 'ingested_at']},
                    'ingested_at': event['ingested_at']
                }
                processed_events.append(processed_event)
                
            print(f"✅ Downloaded {len(processed_events)} telemetry events")
            return processed_events
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error downloading telemetry: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing telemetry JSON: {e}")
            return []
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return []


def save_json_file(data: List[Dict[str, Any]], filepath: str) -> bool:
    """Save data to JSON file"""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"✅ Saved {len(data)} records to {filepath}")
        return True
    except Exception as e:
        print(f"❌ Error saving {filepath}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Fetch PUBG telemetry data from API")
    parser.add_argument("--api-key", required=True, help="PUBG API key")
    parser.add_argument("--players", required=True, nargs='+', 
                       help="Player names to fetch data for")
    parser.add_argument("--platform", default="steam", 
                       choices=['steam', 'xbox', 'psn'],
                       help="Gaming platform")
    parser.add_argument("--output-dir", default="./pubg_data",
                       help="Output directory for data files")
    parser.add_argument("--max-matches", type=int, default=5,
                       help="Maximum matches to fetch per player")
    
    args = parser.parse_args()
    
    print("🎮 PUBG API Telemetry Fetcher")
    print(f"Platform: {args.platform}")
    print(f"Players: {', '.join(args.players)}")
    print(f"Max matches per player: {args.max_matches}")
    print(f"Output directory: {args.output_dir}")
    print()
    
    # Initialize API client
    client = PUBGAPIClient(args.api_key, args.platform)
    
    # Step 1: Get player data
    print("📋 Step 1: Fetching player data...")
    players = client.get_players_by_names(args.players)
    
    if not players:
        print("❌ No players found. Check player names and platform.")
        sys.exit(1)
    
    # Step 2: Get matches for each player
    print("\n🎯 Step 2: Fetching player matches...")
    all_match_ids = set()
    
    for player in players:
        match_ids = client.get_player_matches(player)
        # Limit matches per player
        match_ids = match_ids[:args.max_matches]
        all_match_ids.update(match_ids)
        
        if not match_ids:
            print(f"⚠️  No matches found for {player['player_name']}")
    
    print(f"✅ Total unique matches to fetch: {len(all_match_ids)}")
    
    if not all_match_ids:
        print("❌ No matches found for any players.")
        sys.exit(1)
    
    # Step 3: Get match data and telemetry
    print("\n📊 Step 3: Fetching match data and telemetry...")
    matches = []
    all_telemetry_events = []
    
    for i, match_id in enumerate(all_match_ids, 1):
        print(f"\n--- Processing match {i}/{len(all_match_ids)}: {match_id} ---")
        
        # Get match metadata
        match_data = client.get_match_data(match_id)
        if not match_data:
            print(f"⚠️  Skipping match {match_id} - could not fetch data")
            continue
            
        matches.append(match_data)
        
        # Download telemetry data
        if match_data.get('telemetry_url'):
            telemetry_events = client.download_telemetry_data(
                match_data['telemetry_url'], 
                match_id
            )
            all_telemetry_events.extend(telemetry_events)
        else:
            print(f"⚠️  No telemetry URL for match {match_id}")
    
    # Step 4: Save data files
    print(f"\n💾 Step 4: Saving data files...")
    
    success = True
    success &= save_json_file(players, f"{args.output_dir}/players.json")
    success &= save_json_file(matches, f"{args.output_dir}/matches.json") 
    success &= save_json_file(all_telemetry_events, f"{args.output_dir}/telemetry_events.json")
    
    if success:
        print(f"\n🎉 Successfully fetched PUBG data!")
        print(f"📈 Summary:")
        print(f"   Players: {len(players)}")
        print(f"   Matches: {len(matches)}")
        print(f"   Telemetry events: {len(all_telemetry_events)}")
        print(f"\n📁 Files saved to:")
        print(f"   {args.output_dir}/players.json")
        print(f"   {args.output_dir}/matches.json")
        print(f"   {args.output_dir}/telemetry_events.json")
        print(f"\n💡 Next steps:")
        print(f"   1. Load these files into your data warehouse")
        print(f"   2. Update DBT sources to point to the raw tables")
        print(f"   3. Run: dbt run")
    else:
        print("\n❌ Some files failed to save")
        sys.exit(1)


if __name__ == "__main__":
    main()