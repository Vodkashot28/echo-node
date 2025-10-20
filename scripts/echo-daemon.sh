#!/bin/bash

# ==============================================================================
# echo-daemon.sh - Script to build and start the EchoMesh Rust Daemon
#
# Usage: ./scripts/echo-daemon.sh
# ==============================================================================

# --- Configuration ---
DAEMON_DIR="daemon"
OUTPUT_LOG="../nohup.out" # Log file in the project root

# Check if the daemon directory exists
if [ ! -d "$DAEMON_DIR" ]; then
    echo "❌ Error: Daemon directory '$DAEMON_DIR' not found."
    exit 1
fi

echo "--- EchoMesh Daemon Ritual ---"

# --- 1. Build the Rust Daemon ---
echo "1. Building the Rust Daemon..."
# Change to the daemon directory to run cargo build
pushd "$DAEMON_DIR" > /dev/null

# Build the release version for performance
cargo build --release

# Check if the build was successful
if [ $? -ne 0 ]; then
    echo "❌ Build failed. Please check the Cargo.toml and src/main.rs."
    popd > /dev/null
    exit 1
fi

# The binary path relative to the daemon directory
DAEMON_BINARY="./target/release/daemon"

# Check if the binary was created
if [ ! -f "$DAEMON_BINARY" ]; then
    echo "❌ Daemon binary not found at '$DAEMON_BINARY' after build."
    popd > /dev/null
    exit 1
fi

echo "✅ Daemon built successfully."

# --- 2. Start the Daemon in the Background ---
echo "2. Starting the Daemon (nohup)..."

# Check if a daemon process is already running (simple check based on name)
if pgrep -f "target/release/daemon" > /dev/null; then
    echo "⚠️ A daemon process is already running. Please stop it first if you wish to restart."
    popd > /dev/null
    exit 0
fi

# Use nohup to run the daemon in the background, redirecting output
# We use 'exec' to replace the current shell process with the daemon, but 
# since we want to background it, a simple nohup & is better.
# We redirect the output to the root nohup.out
# Note: The 'daemon' binary is executed from *inside* the 'daemon' directory.
# The `get_project_root` in your Rust code is designed to handle this.
nohup "$DAEMON_BINARY" > "$OUTPUT_LOG" 2>&1 &

# Store the Process ID (PID) of the background job
PID=$!

popd > /dev/null # Go back to the root directory

echo "✅ Daemon started with PID $PID."
echo "   Output is logged to: $OUTPUT_LOG"
echo "   Use 'tail -f $OUTPUT_LOG' to monitor the flow of value."
echo "   Use 'kill $PID' to stop the daemon."

# Make the script executable
chmod +x scripts/echo-daemon.sh
