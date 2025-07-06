#!/usr/bin/env python3
"""
PUBG Telemetry Data Generator (Local Version)

This script generates realistic PUBG telemetry data and saves it locally as JSON files.
Perfect for testing the DBT project without needing AWS S3.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
import argparse
import os
from pathlib import Path


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
    
    def __init__(self, num_matches: int = 5, num_players: int = 50, events_per_match: int = 300):
        self.num_matches = num_matches
        self.num_players = num_players
        self.events_per_match = events_per_match
        self.players = []
        self.matches = []
        
    def generate_players(self) -> List[Dict[str, Any]]:
        """Generate player data"""
        players = []
        
        for i in range(self.num_players):
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
        
        for i in range(self.num_matches):
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
                "telemetry_url": f"https://telemetry-cdn.pubg.com/{str(uuid.uuid4())}-telemetry.json",
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
    
    def generate_telemetry_events(self) -> List[Dict[str, Any]]:
        """Generate telemetry events for all matches"""
        events = []
        
        event_type_weights = {
            "LogPlayerPosition": 0.3,
            "LogItemPickup": 0.2,
            "LogPlayerTakeDamage": 0.15,
            "LogPlayerKillV2": 0.05,
            "LogItemDrop": 0.1,
            "LogItemEquip": 0.1,
            "LogHeal": 0.05,
            "LogPlayerAttack": 0.05
        }
        
        for match in self.matches:
            match_start = datetime.fromisoformat(match["created_at"])
            
            # Generate events throughout the match
            for i in range(self.events_per_match):
                # Random timestamp within match duration
                event_time = match_start + timedelta(
                    seconds=random.uniform(0, match["duration"])
                )
                
                # Choose random event type
                event_type = random.choices(
                    list(event_type_weights.keys()),
                    weights=list(event_type_weights.values())
                )[0]
                
                # Choose random player and location
                player = random.choice(self.players)
                location = self._get_random_location(match["map_name"])
                character = self._generate_character_data(player, location)
                
                # Base event structure
                event = {
                    "_D": event_time.isoformat(),
                    "_T": event_type,
                    "common": {"isGame": random.uniform(1.0, 3.0)},
                    "match_id": match["match_id"],
                    "ingested_at": datetime.now().isoformat()
                }
                
                # Event-specific data
                if event_type == "LogPlayerKillV2":
                    victim = random.choice(self.players)
                    victim_location = self._get_random_location(match["map_name"])
                    event["event_data"] = {
                        "attackId": random.randint(1000, 9999),
                        "killer": character,
                        "victim": self._generate_character_data(victim, victim_location),
                        "damageReason": "ArmShot",
                        "damageTypeCategory": "Damage_Gun",
                        "damageCauserName": "Item_Weapon_AK47_C",
                        "distance": random.uniform(10, 500),
                        "damage": random.uniform(80, 120)
                    }
                elif event_type == "LogPlayerTakeDamage":
                    attacker = random.choice(self.players)
                    attacker_location = self._get_random_location(match["map_name"])
                    event["event_data"] = {
                        "attackId": random.randint(1000, 9999),
                        "attacker": self._generate_character_data(attacker, attacker_location),
                        "victim": character,
                        "damageTypeCategory": "Damage_Gun",
                        "damageReason": "ArmShot",
                        "damage": random.uniform(10, 80),
                        "distance": random.uniform(10, 500)
                    }
                elif event_type == "LogItemPickup":
                    item = self._get_random_item()
                    event["event_data"] = {
                        "character": character,
                        "item": {
                            **item,
                            "stackCount": random.randint(1, 10)
                        }
                    }
                elif event_type == "LogPlayerPosition":
                    event["event_data"] = {
                        "character": character,
                        "elapsedTime": random.randint(0, 2400),
                        "numAlivePlayers": random.randint(10, 100)
                    }
                else:
                    # Generic event structure
                    event["event_data"] = {
                        "character": character
                    }
                    if event_type in ["LogItemDrop", "LogItemEquip"]:
                        event["event_data"]["item"] = self._get_random_item()
                
                events.append(event)
        
        return events


def save_json_file(data: List[Dict[str, Any]], filepath: str) -> bool:
    """Save data to JSON file"""
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"✅ Saved {len(data)} records to {filepath}")
        return True
    except Exception as e:
        print(f"❌ Error saving {filepath}: {e}")
        return False


def save_jsonl_file(data: List[Dict[str, Any]], filepath: str) -> bool:
    """Save data to JSONL file (one JSON object per line)"""
    try:
        with open(filepath, 'w') as f:
            for record in data:
                f.write(json.dumps(record, default=str) + '\n')
        print(f"✅ Saved {len(data)} records to {filepath}")
        return True
    except Exception as e:
        print(f"❌ Error saving {filepath}: {e}")
        return False


def main():
    """Main function to generate PUBG data locally"""
    parser = argparse.ArgumentParser(description="Generate PUBG telemetry data locally")
    parser.add_argument("--output-dir", default="./sample_data", help="Output directory")
    parser.add_argument("--matches", type=int, default=5, help="Number of matches to generate")
    parser.add_argument("--players", type=int, default=50, help="Number of players to generate")
    parser.add_argument("--events", type=int, default=300, help="Events per match")
    parser.add_argument("--format", choices=["json", "jsonl"], default="json", 
                       help="Output format (json or jsonl)")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    print(f"🎮 Generating PUBG telemetry data:")
    print(f"   Matches: {args.matches}")
    print(f"   Players: {args.players}")
    print(f"   Events per match: {args.events}")
    print(f"   Output directory: {output_dir}")
    print(f"   Format: {args.format}")
    
    # Generate data
    generator = PUBGDataGenerator(
        num_matches=args.matches,
        num_players=args.players, 
        events_per_match=args.events
    )
    
    print("\n📊 Generating data...")
    players = generator.generate_players()
    matches = generator.generate_matches()
    events = generator.generate_telemetry_events()
    
    print(f"\n📈 Generated:")
    print(f"   {len(players)} players")
    print(f"   {len(matches)} matches") 
    print(f"   {len(events)} telemetry events")
    
    # Save files
    file_ext = args.format
    save_func = save_jsonl_file if args.format == "jsonl" else save_json_file
    
    print(f"\n💾 Saving files...")
    success = True
    success &= save_func(players, output_dir / f"players.{file_ext}")
    success &= save_func(matches, output_dir / f"matches.{file_ext}")
    success &= save_func(events, output_dir / f"telemetry_events.{file_ext}")
    
    if success:
        print(f"\n🎉 Successfully generated PUBG telemetry data!")
        print(f"\nFiles created:")
        print(f"   {output_dir}/players.{file_ext}")
        print(f"   {output_dir}/matches.{file_ext}")
        print(f"   {output_dir}/telemetry_events.{file_ext}")
        print(f"\n💡 You can now use these files to test your DBT project!")
    else:
        print(f"\n❌ Some files failed to save")


if __name__ == "__main__":
    main()