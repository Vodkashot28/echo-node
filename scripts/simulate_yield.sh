#!/bin/bash

# ==============================================================================
# simulate_yield.sh - A Ritual Helper script to run the daemon for a fixed time 
#                     and simulate the flow of value for testing purposes.
#
# Usage: ./scripts/simulate_yield.sh [DURATION_SECONDS]
# Example: ./scripts/simulate_yield.sh 30 (runs for 30 seconds)
# ==============================================================================

# --- Configuration ---
DAEMON_DIR="daemon"
DEFAULT_DURATION=10 # Default simulation time in seconds

# Get duration from arguments, or use default
DURATION=${1:-$DEFAULT_DURATION}

echo "--- EchoMesh Yield Simulation Ritual ---"
echo "Target Duration: $DURATION seconds."

# --- 1. Build the Rust Daemon (Ensure it's up to date) ---
echo "1. Ensuring Daemon is built..."
pushd "$DAEMON_DIR" > /dev/null

cargo build --release

if [ $? -ne 0 ]; then
    echo "❌ Build failed. Cannot run simulation."
    popd > /dev/null
    exit 1
fi

DAEMON_BINARY="./target/release/daemon"
popd > /dev/null

echo "✅ Daemon ready."

# --- 2. Run the Daemon for the specified duration ---
echo "2. Starting simulation..."

# Run the daemon in the background
"$DAEMON_DIR/$DAEMON_BINARY" &

# Store the PID of the simulation
SIMULATION_PID=$!

echo "   Simulation PID: $SIMULATION_PID. Monitoring value flow..."

# Wait for the specified duration
sleep "$DURATION"

# --- 3. Stop the Simulation ---
echo "\n3. Simulation Complete. Halting ritual..."
kill "$SIMULATION_PID" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Daemon (PID $SIMULATION_PID) stopped successfully."
else
    echo "⚠️ Warning: Daemon process may have already exited or kill command failed."
fi

# Optional: Run the parser after the simulation
echo "\n--- Analyzing Simulation Results ---"
./scripts/parse_ledger.py

echo "--- Ritual End ---"

# Make the script executable
chmod +x scripts/simulate_yield.sh
