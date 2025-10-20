#!/bin/bash

# --- Environment Setup and Hardening ---
# Explicitly export HOME for shell path resolution consistency.
export HOME="/data/data/com.termux/files/home" 

# NOTE: The IPFS client relies on a running daemon, which should be started separately.
# If your IPFS repository is not in the default location (~/.ipfs),
# you must uncomment and adjust the line below:
# export IPFS_PATH="$HOME/.ipfs"

# Temporary placeholder for BW_MB if it's not set externally. 
# REMOVE the 'export BW_MB=...' line if you are passing the value from the parent script.
# For testing stability, let's assume a default value if missing:
BW_MB=${BW_MB:-100.0} 

# --- Configuration Paths ---
LEDGER="$HOME/echo-node/echo-site/resurrection_ledger.log"
SITE_DIR="$HOME/echo-node/echo-site"
EVENT_FILE="$HOME/echo-node/echo-site/event.txt"


# --- Step 0: Simulate bandwidth and yield ---
BANDWIDTH_MB=$(echo "$BW_MB" | awk '{printf "%.2f", $1}')
YIELD=$(echo "$BANDWIDTH_MB * 0.05" | bc)

# --- Step 1: Update site contents ---
# REMOVED the redundant 'cp' command that caused the "same file" warning.
# Assuming any updates to event.txt are done in place before running this script.

# --- Step 2: Add folder to IPFS (CRITICAL SECTION) ---
# This requires the IPFS daemon to be running and the client configured.
echo "Adding $SITE_DIR to IPFS..."
CID_V0=$(ipfs add -r -Q "$SITE_DIR")

# --- Step 3: Convert to v1 CID (base32) ---
if [ -z "$CID_V0" ]; then
    echo "Error: IPFS add failed. CID_V0 is empty. Check daemon status." >&2
    exit 1
fi
CID_V1=$(ipfs cid format -v 1 -b base32 "$CID_V0")

# --- Step 4: Publish to IPNS ---
echo "Publishing CID $CID_V0 to IPNS..."
IPNS_ID=$(ipfs name publish "$CID_V0" | awk '{print $NF}')

# --- Step 5: Log to resurrection ledger ---
echo "$(date) | CIDv0: $CID_V0 | CIDv1: $CID_V1 | IPNS: $IPNS_ID | Bandwidth: ${BANDWIDTH_MB}MB | Yield: ${YIELD} ECHO" >> "$LEDGER"

# --- Step 6: Output and pin ---
echo "✅ Published to IPNS: $IPNS_ID"
echo "✅ CIDv1 (base32): $CID_V1"
ipfs pin add "$CID_V0"
