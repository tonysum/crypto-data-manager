#!/bin/bash

# Main startup script for Crypto Data Manager

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --docker    Start using Docker Compose (Recommended for isolated environment)"
    echo "  --local     Start in local development mode (Recommended for active dev)"
    echo "  --help      Show this help message"
}

# Default mode
MODE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --docker)
            MODE="docker"
            shift
            ;;
        --local)
            MODE="local"
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

# If no mode specified, prompt the user
if [[ -z "$MODE" ]]; then
    echo -e "${BLUE}Welcome to Crypto Data Manager Startup Script!${NC}"
    echo "Please choose a startup mode:"
    echo "1) Local Development (Required: Python/uv, Node.js)"
    echo "2) Docker Compose (Required: Docker)"
    read -p "Enter Choice [1 or 2]: " choice
    
    case $choice in
        1) MODE="local" ;;
        2) MODE="docker" ;;
        *) echo -e "${RED}Invalid choice. Exiting.${NC}"; exit 1 ;;
    esac
fi

# Make scripts executable
chmod +x "$SCRIPT_DIR/start-docker.sh"
chmod +x "$SCRIPT_DIR/start-local.sh"

# Run the selected mode
if [[ "$MODE" == "docker" ]]; then
    bash "$SCRIPT_DIR/start-docker.sh"
else
    bash "$SCRIPT_DIR/start-local.sh"
fi
