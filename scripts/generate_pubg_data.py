#!/usr/bin/env python3
"""
PUBG Telemetry Data Generator and S3 Uploader

This script generates realistic PUBG telemetry data including:
- Telemetry events (kills, damage, item interactions, etc.)
- Match metadata
- Player information

The generated data matches the schema expected by the DBT project.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
import boto3
from botocore.exceptions import ClientError
import argparse
import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for data generation"""
    num_matches: int = 10
    num_players: int = 100
    events_per_match: int = 500
    s3_bucket: str = "pubg-telemetry-data"
    s3_prefix: str = "raw-data/"
    aws_region: str = "us-east-1"


class PUBGDataGenerator:
    """Generates realistic PUBG telemetry data"""
    
    # PUBG maps with their coordinate ranges (in centimeters)
    MAPS = {
        "Erangel": {"max_x": 816000, "max_y": 816000},
        "Miramar": {"max_x": 816000, "max_y": 816000},
        "Sanhok": {"max_x": 408000, "max_y": 408000},
        "Vikendi": {"max_x": 816000, "max_y": 816000},
        "Karakin": {"max_x": 204000, "max_y": 204000},
        "Taego": {"max_x": 816000, "max_y": 816000},
        "Deston": {"max_x": 816000, "max_y": 816000},
        "Haven": {"max_x": 102000, "max_y": 102000}
    }
    
    GAME_MODES = ["Solo", "Duo", "Squad"]
    PLATFORMS = ["Steam", "Xbox", "PlayStation"]
    REGIONS = ["NA", "EU", "AS", "OC", "SA"]
    
    # Telemetry event types from PUBG API documentation
    EVENT_TYPES = [
        "LogPlayerKillV2", "LogPlayerAttack", "LogPlayerTakeDamage", 
        "LogPlayerMakeGroggy", "LogPlayerRevive", "LogPlayerPosition",
        "LogItemPickup", "LogItemDrop", "LogItemEquip", "LogItemUse",
        "LogVehicleRide", "LogVehicleLeave", "LogVehicleDamage",
        "LogMatchStart", "LogMatchEnd", "LogPhaseChange",
        "LogHeal", "LogItemAttach", "LogItemDetach"
    ]
    
    # Item categories and IDs
    ITEMS = {
        "Weapon": {
            "Main": [
                "Item_Weapon_AK47_C", "Item_Weapon_M416_C", "Item_Weapon_SCAR-L_C",
                "Item_Weapon_M16A4_C", "Item_Weapon_Kar98k_C", "Item_Weapon_AWM_C"
            ],
            "Handgun": [
                "Item_Weapon_P92_C", "Item_Weapon_P1911_C", "Item_Weapon_Glock_C"
            ]
        },
        "Equipment": {
            "Head": [
                "Item_Head_E_01_Lv1_C", "Item_Head_E_02_Lv2_C", "Item_Head_E_03_Lv3_C"
            ],
            "Torso": [
                "Item_Armor_E_01_Lv1_C", "Item_Armor_E_02_Lv2_C", "Item_Armor_E_03_Lv3_C"
            ]
        },
        "Use": {
            "Heal": [
                "Item_Heal_FirstAid_C", "Item_Heal_Medkit_C", "Item_Heal_Bandage_C"
            ],
            "Boost": [
                "Item_Boost_EnergyDrink_C", "Item_Boost_PainKiller_C", "Item_Boost_Adrenaline_C"
            ]
        },
        "Attachment": {
            "None": [
                "Item_Attach_Weapon_Muzzle_Compensator_C",
                "Item_Attach_Weapon_Upper_ACOG_01_C",
                "Item_Attach_Weapon_Stock_AR_Composite_C"
            ]
        }
    }
    
    def __init__(self, config: Config):
        self.config = config
        self.players = []
        self.matches = []
        self.telemetry_events = []
        
    def generate_players(self) -> List[Dict[str, Any]]:
        """Generate player data"""
        players = []
        
        for i in range(self.config.num_players):
            player = {
                "account_id": str(uuid.uuid4()),
                "player_name": f"Player_{i:04d}",
                "platform": random.choice(self.PLATFORMS),
                "region": random.choice(self.REGIONS),
                "ingested_at": datetime.now().isoformat()
            }
            players.append(player)
            
        self.players = players
        return players
    
    def generate_matches(self) -> List[Dict[str, Any]]:
        """Generate match metadata"""
        matches = []
        
        for i in range(self.config.num_matches):
            map_name = random.choice(list(self.MAPS.keys()))
            created_at = datetime.now() - timedelta(
                hours=random.randint(1, 24 * 7)  # Last week
            )
            duration = random.randint(1200, 2400)  # 20-40 minutes
            
            match = {
                "match_id": str(uuid.uuid4()),
                "map_name": map_name,
                "game_mode": random.choice(self.GAME_MODES),
                "match_type": "Official",
                "created_at": created_at.isoformat(),
                "duration": duration,
                "telemetry_url": f"https://telemetry-cdn.pubg.com/{match['match_id']}-telemetry.json",
                "ingested_at": datetime.now().isoformat()
            }
            matches.append(match)
            
        self.matches = matches
        return matches
    
    def _get_random_location(self, map_name: str) -> Dict[str, float]:
        """Get random location coordinates for a given map"""
        map_bounds = self.MAPS[map_name]
        return {
            "x": random.uniform(0, map_bounds["max_x"]),
            "y": random.uniform(0, map_bounds["max_y"]),
            "z": random.uniform(0, 1000)  # Elevation
        }
    
    def _get_random_item(self) -> Dict[str, str]:
        """Get random item with category and subcategory"""
        category = random.choice(list(self.ITEMS.keys()))
        subcategory = random.choice(list(self.ITEMS[category].keys()))
        item_id = random.choice(self.ITEMS[category][subcategory])
        
        return {
            "itemId": item_id,
            "category": category,
            "subCategory": subcategory
        }
    
    def _generate_character_data(self, player: Dict[str, Any], 
                               location: Dict[str, float]) -> Dict[str, Any]:
        """Generate character object for telemetry events"""
        return {
            "accountId": player["account_id"],
            "name": player["player_name"],
            "teamId": random.randint(1, 25),
            "health": random.uniform(0, 100),
            "location": location,
            "ranking": random.randint(1, 100),
            "isInBlueZone": random.choice([True, False]),
            "isInRedZone": random.choice([True, False])
        }
    
    def _generate_kill_event(self, match: Dict[str, Any], 
                           timestamp: datetime) -> Dict[str, Any]:
        """Generate LogPlayerKillV2 event"""
        killer = random.choice(self.players)
        victim = random.choice(self.players)
        location = self._get_random_location(match["map_name"])
        
        return {
            "_D": timestamp.isoformat(),
            "_T": "LogPlayerKillV2",
            "common": {"isGame": random.uniform(1.0, 3.0)},
            "match_id": match["match_id"],
            "event_data": {
                "attackId": random.randint(1000, 9999),
                "killer": self._generate_character_data(killer, location),
                "victim": self._generate_character_data(victim, location),
                "damageReason": "ArmShot",
                "damageTypeCategory": "Damage_Gun",
                "damageCauserName": "Item_Weapon_AK47_C",
                "distance": random.uniform(10, 500),
                "damage": random.uniform(80, 120)
            },
            "ingested_at": datetime.now().isoformat()
        }
    
    def _generate_damage_event(self, match: Dict[str, Any], 
                             timestamp: datetime) -> Dict[str, Any]:
        """Generate LogPlayerTakeDamage event"""
        attacker = random.choice(self.players)
        victim = random.choice(self.players)
        location = self._get_random_location(match["map_name"])
        
        return {
            "_D": timestamp.isoformat(),
            "_T": "LogPlayerTakeDamage",
            "common": {"isGame": random.uniform(1.0, 3.0)},
            "match_id": match["match_id"],
            "event_data": {
                "attackId": random.randint(1000, 9999),
                "attacker": self._generate_character_data(attacker, location),
                "victim": self._generate_character_data(victim, location),
                "damageTypeCategory": "Damage_Gun",
                "damageReason": "ArmShot",
                "damage": random.uniform(10, 80),
                "distance": random.uniform(10, 500)
            },
            "ingested_at": datetime.now().isoformat()
        }
    
    def _generate_item_pickup_event(self, match: Dict[str, Any], 
                                  timestamp: datetime) -> Dict[str, Any]:
        """Generate LogItemPickup event"""
        player = random.choice(self.players)
        location = self._get_random_location(match["map_name"])
        item = self._get_random_item()
        
        return {
            "_D": timestamp.isoformat(),
            "_T": "LogItemPickup",
            "common": {"isGame": random.uniform(1.0, 3.0)},
            "match_id": match["match_id"],
            "event_data": {
                "character": self._generate_character_data(player, location),
                "item": {
                    **item,
                    "stackCount": random.randint(1, 10)
                }
            },
            "ingested_at": datetime.now().isoformat()
        }
    
    def _generate_position_event(self, match: Dict[str, Any], 
                               timestamp: datetime) -> Dict[str, Any]:
        """Generate LogPlayerPosition event"""
        player = random.choice(self.players)
        location = self._get_random_location(match["map_name"])
        
        return {
            "_D": timestamp.isoformat(),
            "_T": "LogPlayerPosition",
            "common": {"isGame": random.uniform(1.0, 3.0)},
            "match_id": match["match_id"],
            "event_data": {
                "character": self._generate_character_data(player, location),
                "elapsedTime": random.randint(0, 2400),
                "numAlivePlayers": random.randint(10, 100)
            },
            "ingested_at": datetime.now().isoformat()
        }
    
    def _generate_match_start_event(self, match: Dict[str, Any], 
                                  timestamp: datetime) -> Dict[str, Any]:
        """Generate LogMatchStart event"""
        # Generate multiple characters for match start
        characters = []
        for _ in range(random.randint(50, 100)):
            player = random.choice(self.players)
            location = self._get_random_location(match["map_name"])
            characters.append({
                "character": self._generate_character_data(player, location),
                "primaryWeaponFirst": "",
                "primaryWeaponSecond": "",
                "secondaryWeapon": "",
                "spawnKitIndex": 0
            })
        
        return {
            "_D": timestamp.isoformat(),
            "_T": "LogMatchStart",
            "common": {"isGame": 0.5},
            "match_id": match["match_id"],
            "event_data": {
                "mapName": match["map_name"],
                "weatherId": "Clear",
                "characters": characters,
                "cameraViewBehaviour": "FPP",
                "teamSize": 1 if match["game_mode"] == "Solo" else (2 if match["game_mode"] == "Duo" else 4),
                "isCustomGame": False,
                "isEventMode": False
            },
            "ingested_at": datetime.now().isoformat()
        }
    
    def generate_telemetry_events(self) -> List[Dict[str, Any]]:
        """Generate telemetry events for all matches"""
        events = []
        
        for match in self.matches:
            match_start = datetime.fromisoformat(match["created_at"])
            match_duration = timedelta(seconds=match["duration"])
            
            # Generate match start event
            events.append(self._generate_match_start_event(match, match_start))
            
            # Generate events throughout the match
            for i in range(self.config.events_per_match):
                # Random timestamp within match duration
                event_time = match_start + timedelta(
                    seconds=random.uniform(0, match["duration"])
                )
                
                # Choose event type with weighted probabilities
                event_type_weights = {
                    "LogPlayerPosition": 0.3,
                    "LogItemPickup": 0.2,
                    "LogPlayerTakeDamage": 0.15,
                    "LogPlayerKillV2": 0.05,
                    "LogItemDrop": 0.1,
                    "LogItemEquip": 0.1,
                    "LogHeal": 0.05,
                    "LogVehicleRide": 0.05
                }
                
                event_type = random.choices(
                    list(event_type_weights.keys()),
                    weights=list(event_type_weights.values())
                )[0]
                
                # Generate specific event based on type
                if event_type == "LogPlayerKillV2":
                    event = self._generate_kill_event(match, event_time)
                elif event_type == "LogPlayerTakeDamage":
                    event = self._generate_damage_event(match, event_time)
                elif event_type == "LogItemPickup":
                    event = self._generate_item_pickup_event(match, event_time)
                elif event_type == "LogPlayerPosition":
                    event = self._generate_position_event(match, event_time)
                else:
                    # Generic event for other types
                    player = random.choice(self.players)
                    location = self._get_random_location(match["map_name"])
                    event = {
                        "_D": event_time.isoformat(),
                        "_T": event_type,
                        "common": {"isGame": random.uniform(1.0, 3.0)},
                        "match_id": match["match_id"],
                        "event_data": {
                            "character": self._generate_character_data(player, location)
                        },
                        "ingested_at": datetime.now().isoformat()
                    }
                
                events.append(event)
        
        self.telemetry_events = events
        return events


class S3Uploader:
    """Handles uploading data to S3"""
    
    def __init__(self, bucket_name: str, region: str = "us-east-1"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region)
        
    def create_bucket_if_not_exists(self) -> bool:
        """Create S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} already exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    print(f"Created bucket {self.bucket_name}")
                    return True
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
                    return False
            else:
                print(f"Error checking bucket: {e}")
                return False
    
    def upload_json_data(self, data: List[Dict[str, Any]], 
                        key: str) -> bool:
        """Upload JSON data to S3"""
        try:
            json_data = json.dumps(data, indent=2, default=str)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data,
                ContentType='application/json'
            )
            
            print(f"Uploaded {len(data)} records to s3://{self.bucket_name}/{key}")
            return True
            
        except ClientError as e:
            print(f"Error uploading to S3: {e}")
            return False
    
    def upload_jsonl_data(self, data: List[Dict[str, Any]], 
                         key: str) -> bool:
        """Upload data as JSON Lines format (one JSON object per line)"""
        try:
            jsonl_data = '\n'.join([json.dumps(record, default=str) for record in data])
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=jsonl_data,
                ContentType='application/x-ndjson'
            )
            
            print(f"Uploaded {len(data)} records to s3://{self.bucket_name}/{key}")
            return True
            
        except ClientError as e:
            print(f"Error uploading to S3: {e}")
            return False


def main():
    """Main function to generate and upload PUBG data"""
    parser = argparse.ArgumentParser(description="Generate PUBG telemetry data and upload to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="raw-data/", help="S3 key prefix")
    parser.add_argument("--matches", type=int, default=10, help="Number of matches to generate")
    parser.add_argument("--players", type=int, default=100, help="Number of players to generate")
    parser.add_argument("--events", type=int, default=500, help="Events per match")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--format", choices=["json", "jsonl"], default="jsonl", 
                       help="Output format (json or jsonl)")
    
    args = parser.parse_args()
    
    # Configure data generation
    config = Config(
        num_matches=args.matches,
        num_players=args.players,
        events_per_match=args.events,
        s3_bucket=args.bucket,
        s3_prefix=args.prefix,
        aws_region=args.region
    )
    
    print(f"Generating PUBG data with config:")
    print(f"  Matches: {config.num_matches}")
    print(f"  Players: {config.num_players}")
    print(f"  Events per match: {config.events_per_match}")
    print(f"  S3 Bucket: {config.s3_bucket}")
    print(f"  Format: {args.format}")
    
    # Generate data
    generator = PUBGDataGenerator(config)
    
    print("\nGenerating players...")
    players = generator.generate_players()
    
    print("Generating matches...")
    matches = generator.generate_matches()
    
    print("Generating telemetry events...")
    events = generator.generate_telemetry_events()
    
    print(f"\nGenerated:")
    print(f"  {len(players)} players")
    print(f"  {len(matches)} matches")
    print(f"  {len(events)} telemetry events")
    
    # Upload to S3
    uploader = S3Uploader(config.s3_bucket, config.aws_region)
    
    if not uploader.create_bucket_if_not_exists():
        print("Failed to create/access S3 bucket")
        return
    
    print("\nUploading to S3...")
    
    upload_func = uploader.upload_jsonl_data if args.format == "jsonl" else uploader.upload_json_data
    file_ext = "jsonl" if args.format == "jsonl" else "json"
    
    # Upload each dataset
    success = True
    success &= upload_func(players, f"{config.s3_prefix}players.{file_ext}")
    success &= upload_func(matches, f"{config.s3_prefix}matches.{file_ext}")
    success &= upload_func(events, f"{config.s3_prefix}telemetry_events.{file_ext}")
    
    if success:
        print("\n✅ Successfully generated and uploaded PUBG telemetry data!")
        print(f"\nData available at:")
        print(f"  s3://{config.s3_bucket}/{config.s3_prefix}players.{file_ext}")
        print(f"  s3://{config.s3_bucket}/{config.s3_prefix}matches.{file_ext}")
        print(f"  s3://{config.s3_bucket}/{config.s3_prefix}telemetry_events.{file_ext}")
    else:
        print("\n❌ Some uploads failed")


if __name__ == "__main__":
    main()