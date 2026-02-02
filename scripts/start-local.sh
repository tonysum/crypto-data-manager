#!/bin/bash

# Local startup script for Crypto Data Manager

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}Starting Crypto Data Manager in Local mode...${NC}"

# Cleanup function to kill background processes
cleanup() {
    echo -e "\n${BLUE}Stopping services...${NC}"
    if [ ! -z "$TAIL_PID" ] && kill -0 $TAIL_PID 2>/dev/null; then
        kill $TAIL_PID 2>/dev/null
    fi
    if [ ! -z "$BACKEND_PID" ] && kill -0 $BACKEND_PID 2>/dev/null; then
        kill $BACKEND_PID 2>/dev/null
        echo -e "${BLUE}Backend stopped.${NC}"
    fi
    exit
}

# Trap Ctrl+C (SIGINT) and cleanup
trap cleanup SIGINT

# 1. Dependency Checks
echo -e "${BLUE}Checking dependencies...${NC}"

if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}Warning: 'uv' is not installed. Will try to use 'python' instead.${NC}"
    PYTHON_CMD="python"
else
    PYTHON_CMD="uv run python"
fi

if ! command -v npm &> /dev/null; then
    echo -e "${RED}Error: npm is not installed. Please install Node.js and npm.${NC}"
    exit 1
fi

# 2. Environment Setup
if [ ! -f "$ROOT_DIR/.env" ]; then
    echo -e "${RED}Error: .env file not found in root directory.${NC}"
    echo -e "Please create a .env file with your BINANCE_API_KEY and BINANCE_API_SECRET."
    exit 1
fi

# 3. Start Backend
echo -e "${BLUE}Starting Backend...${NC}"
cd "$ROOT_DIR/backend"

# Ensure dependencies are installed BEFORE starting the server
if command -v uv &> /dev/null; then
    echo -e "${BLUE}Ensuring backend dependencies are up to date (this may take a while if building numpy)...${NC}"
    # Trigger uv sync/build in FOREGROUND so user sees progress
    # We don't redirect to /dev/null so user can see 'Building numpy' etc.
    uv run python -c "import sys; print(f'Python {sys.version_info.major}.{sys.version_info.minor} environment ready')"
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}uv sync failed, attempting manual install...${NC}"
        uv pip install -r requirements.txt
    fi
else
    echo -e "${YELLOW}Warning: Ensure your python environment is ready (uv not found).${NC}"
fi

# Run backend in background
echo -e "${BLUE}Launching backend process...${NC}"
> "$ROOT_DIR/backend.log" # Clear old logs
$PYTHON_CMD main.py > "$ROOT_DIR/backend.log" 2>&1 &
BACKEND_PID=$!

# Start tailing logs in background to show progress
tail -f "$ROOT_DIR/backend.log" &
TAIL_PID=$!

echo -e "${GREEN}Backend started with PID $BACKEND_PID. Logs at backend.log${NC}"

# 4. Wait for Backend
echo -e "${BLUE}Waiting for backend to be ready (Port 8001)...${NC}"
MAX_RETRIES=120 # Increased to 2 minutes just in case
RETRY_COUNT=0
until curl --output /dev/null --silent --fail http://localhost:8001; do
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "${RED}Error: Backend failed to start within expected time (120s).${NC}"
        echo -e "${YELLOW}Last 10 lines of backend.log:${NC}"
        tail -n 10 "$ROOT_DIR/backend.log"
        cleanup
    fi
    if ! kill -0 $BACKEND_PID 2>/dev/null; then
        echo -e "${RED}Error: Backend process died unexpectedly.${NC}"
        echo -e "${YELLOW}Last 20 lines of backend.log:${NC}"
        tail -n 20 "$ROOT_DIR/backend.log"
        cleanup
    fi
    sleep 1
    let RETRY_COUNT=RETRY_COUNT+1
done

# Kill tailing process before starting frontend to keep terminal clean
kill $TAIL_PID 2>/dev/null
echo -e "\n${GREEN}Backend is ready!${NC}"

# 5. Start Frontend
echo -e "${BLUE}Starting Frontend...${NC}"
cd "$ROOT_DIR/frontend"

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo -e "${BLUE}Installing frontend dependencies (this may take a while)...${NC}"
    npm install
fi

# Start frontend (blocks the terminal)
npm run dev -- -p 3001
