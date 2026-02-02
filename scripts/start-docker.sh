#!/bin/bash

# Docker startup script for Crypto Data Manager

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}Starting Crypto Data Manager in Docker mode...${NC}"

# Check for docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: docker is not installed.${NC}"
    exit 1
fi

# Check for docker-compose
if ! command -v docker-compose &> /dev/null; then
    # Some systems use 'docker compose' instead of 'docker-compose'
    if ! docker compose version &> /dev/null; then
        echo -e "${RED}Error: docker-compose is not installed.${NC}"
        exit 1
    fi
    DOCKER_COMPOSE_CMD="docker compose"
else
    DOCKER_COMPOSE_CMD="docker-compose"
fi

# Ensure Postgres container is running
echo -e "${BLUE}Ensuring PostgreSQL database is running...${NC}"
if [ ! "$(docker ps -q -f name=crypto_data_manager__postgres_server)" ]; then
    if [ "$(docker ps -aq -f status=exited -f name=crypto_data_manager__postgres_server)" ]; then
        echo -e "${BLUE}Starting existing PostgreSQL container...${NC}"
        docker start crypto_data_manager__postgres_server
    else
        echo -e "${BLUE}Creating new PostgreSQL container...${NC}"
        bash "$ROOT_DIR/create_postgresql.sh"
    fi
else
    echo -e "${GREEN}PostgreSQL container is already running.${NC}"
fi

# Check if .env exists, if not use .env.example if available or warn
if [ ! -f "$ROOT_DIR/.env" ]; then
    if [ -f "$ROOT_DIR/.env.example" ]; then
        echo -e "${BLUE}Creating .env from .env.example...${NC}"
        cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
        echo -e "${RED}Warning: Created .env from .env.example. Please update your API keys in .env${NC}"
    else
        echo -e "${RED}Error: .env file not found in root directory.${NC}"
        echo -e "Please create a .env file with your BINANCE_API_KEY and BINANCE_API_SECRET."
        exit 1
    fi
fi

# Start services
echo -e "${BLUE}Building and starting frontend/backend services...${NC}"
$DOCKER_COMPOSE_CMD up --build
