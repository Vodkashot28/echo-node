#!/bin/bash

# Configurable bandwidth input (in MB)
BW_MB=${1:-256}  # Default to 256MB if not provided

# Paths
LEDGER="resurrection_ledger.log"
EVENT_FILE="event.txt"
SITE_DIR="echo-site"

# Step 1: Calculate yield
BANDWIDTH_MB=$(echo "$BW_MB" | awk '{printf "%.2f", $1}')
YIELD=$(echo "$BANDWIDTH_MB * 0.02" | bc)  # Adjust multiplier as needed

# Step 2: Create event file
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
echo "$TIMESTAMP | Yield Cycle → ${BANDWIDTH_MB}MB → ${YIELD} ECHO" > "$EVENT_FILE"

# Step 3: Inject into site
cp "$EVENT_FILE" "$SITE_DIR/"

# Step 4: Publish to IPFS
CID_V0=$(ipfs add -r -Q "$SITE_DIR")
CID_V1=$(ipfs cid format -v 1 -b base32 "$CID_V0")

# Step 5: Log to ledger
echo "$TIMESTAMP | Bandwidth: ${BANDWIDTH_MB}MB | Yield: ${YIELD} ECHO | CIDv0: $CID_V0 | CIDv1: $CID_V1" >> "$LEDGER"

# Step 6: Output
echo "✅ Yield cycle complete"
echo "🔗 Preview: https://${CID_V1}.ipfs.w3s.link/ec"

