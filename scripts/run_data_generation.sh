#!/bin/bash

# PUBG Telemetry Data Generation Script
# Provides easy options for generating sample data locally or uploading to S3

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
MATCHES=10
PLAYERS=100
EVENTS=500
OUTPUT_DIR="./sample_data"
FORMAT="json"

print_usage() {
    echo -e "${BLUE}🎮 PUBG Telemetry Data Generator${NC}"
    echo ""
    echo "Usage: $0 [OPTIONS] COMMAND"
    echo ""
    echo "Commands:"
    echo "  local     Generate data locally (no AWS required)"
    echo "  s3        Generate and upload data to S3"
    echo ""
    echo "Options:"
    echo "  -m, --matches NUM      Number of matches to generate (default: $MATCHES)"
    echo "  -p, --players NUM      Number of players to generate (default: $PLAYERS)"
    echo "  -e, --events NUM       Events per match (default: $EVENTS)"
    echo "  -o, --output-dir DIR   Output directory for local files (default: $OUTPUT_DIR)"
    echo "  -f, --format FORMAT    Output format: json or jsonl (default: $FORMAT)"
    echo "  -b, --bucket BUCKET    S3 bucket name (required for s3 command)"
    echo "  -r, --region REGION    AWS region (default: us-east-1)"
    echo "  --prefix PREFIX        S3 key prefix (default: raw-data/)"
    echo "  -h, --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 local                                    # Generate small dataset locally"
    echo "  $0 local -m 5 -p 50 -e 200                # Generate custom dataset locally"
    echo "  $0 s3 -b my-pubg-bucket                    # Upload to S3"
    echo "  $0 s3 -b my-bucket -m 20 -p 200 -e 1000   # Large dataset to S3"
}

# Parse command line arguments
COMMAND=""
S3_BUCKET=""
AWS_REGION="us-east-1"
S3_PREFIX="raw-data/"

while [[ $# -gt 0 ]]; do
    case $1 in
        local|s3)
            COMMAND="$1"
            shift
            ;;
        -m|--matches)
            MATCHES="$2"
            shift 2
            ;;
        -p|--players)
            PLAYERS="$2"
            shift 2
            ;;
        -e|--events)
            EVENTS="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -f|--format)
            FORMAT="$2"
            shift 2
            ;;
        -b|--bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        -r|--region)
            AWS_REGION="$2"
            shift 2
            ;;
        --prefix)
            S3_PREFIX="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            print_usage
            exit 1
            ;;
    esac
done

# Check if command is provided
if [[ -z "$COMMAND" ]]; then
    echo -e "${RED}❌ Command required (local or s3)${NC}"
    print_usage
    exit 1
fi

# Check Python installation
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is required but not installed${NC}"
    exit 1
fi

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo -e "${BLUE}🎮 PUBG Telemetry Data Generator${NC}"
echo -e "📊 Configuration:"
echo -e "   Command: ${YELLOW}$COMMAND${NC}"
echo -e "   Matches: ${YELLOW}$MATCHES${NC}"
echo -e "   Players: ${YELLOW}$PLAYERS${NC}"
echo -e "   Events per match: ${YELLOW}$EVENTS${NC}"
echo -e "   Format: ${YELLOW}$FORMAT${NC}"

if [[ "$COMMAND" == "local" ]]; then
    echo -e "   Output directory: ${YELLOW}$OUTPUT_DIR${NC}"
    echo ""
    
    # Check if local script exists
    LOCAL_SCRIPT="$SCRIPT_DIR/generate_local_data.py"
    if [[ ! -f "$LOCAL_SCRIPT" ]]; then
        echo -e "${RED}❌ Local generation script not found: $LOCAL_SCRIPT${NC}"
        exit 1
    fi
    
    # Run local generation
    echo -e "${GREEN}🚀 Generating data locally...${NC}"
    python3 "$LOCAL_SCRIPT" \
        --output-dir "$OUTPUT_DIR" \
        --matches "$MATCHES" \
        --players "$PLAYERS" \
        --events "$EVENTS" \
        --format "$FORMAT"
    
elif [[ "$COMMAND" == "s3" ]]; then
    # Check S3 bucket parameter
    if [[ -z "$S3_BUCKET" ]]; then
        echo -e "${RED}❌ S3 bucket name required for s3 command (-b/--bucket)${NC}"
        exit 1
    fi
    
    echo -e "   S3 bucket: ${YELLOW}$S3_BUCKET${NC}"
    echo -e "   AWS region: ${YELLOW}$AWS_REGION${NC}"
    echo -e "   S3 prefix: ${YELLOW}$S3_PREFIX${NC}"
    echo ""
    
    # Check if S3 script exists
    S3_SCRIPT="$SCRIPT_DIR/generate_pubg_data.py"
    if [[ ! -f "$S3_SCRIPT" ]]; then
        echo -e "${RED}❌ S3 generation script not found: $S3_SCRIPT${NC}"
        exit 1
    fi
    
    # Check if boto3 is installed
    if ! python3 -c "import boto3" 2>/dev/null; then
        echo -e "${YELLOW}⚠️  boto3 not found. Installing dependencies...${NC}"
        if [[ -f "$SCRIPT_DIR/requirements.txt" ]]; then
            pip3 install -r "$SCRIPT_DIR/requirements.txt"
        else
            pip3 install boto3
        fi
    fi
    
    # Check AWS credentials
    if [[ -z "$AWS_ACCESS_KEY_ID" && -z "$AWS_PROFILE" && ! -f "$HOME/.aws/credentials" ]]; then
        echo -e "${YELLOW}⚠️  No AWS credentials found. Make sure to configure AWS CLI or set environment variables.${NC}"
        echo -e "   Options:"
        echo -e "   1. Run: aws configure"
        echo -e "   2. Set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"
        echo -e "   3. Use IAM role (if running on EC2)"
        echo ""
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Run S3 generation
    echo -e "${GREEN}🚀 Generating data and uploading to S3...${NC}"
    python3 "$S3_SCRIPT" \
        --bucket "$S3_BUCKET" \
        --prefix "$S3_PREFIX" \
        --matches "$MATCHES" \
        --players "$PLAYERS" \
        --events "$EVENTS" \
        --region "$AWS_REGION" \
        --format "$FORMAT"
fi

echo ""
echo -e "${GREEN}✨ Data generation complete!${NC}"

if [[ "$COMMAND" == "local" ]]; then
    echo -e "${BLUE}💡 Next steps:${NC}"
    echo -e "   1. Update your DBT sources to point to these files"
    echo -e "   2. Run: dbt run"
    echo -e "   3. Run: dbt test"
    echo -e "   4. Generate docs: dbt docs generate && dbt docs serve"
elif [[ "$COMMAND" == "s3" ]]; then
    echo -e "${BLUE}💡 Next steps:${NC}"
    echo -e "   1. Configure your data warehouse to read from S3"
    echo -e "   2. Update DBT sources to point to the raw tables"
    echo -e "   3. Run: dbt run"
    echo -e "   4. Run: dbt test"
fi