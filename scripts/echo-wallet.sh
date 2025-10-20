#!/bin/bash
# echo-wallet.sh: Fetches the last ECHO earned and updates the wallet balance.

# --- Configuration ---
LEDGER_FILE="../echo-site/resurrection_ledger.log"
WALLET_FILE="../echo-site/echo-wallet.balance"

# --- Main Logic ---

# 1. Get the last successful YIELD_CYCLE line from the ledger
LAST_YIELD_LINE=$(grep 'YIELD_CYCLE' "$LEDGER_FILE" | tail -n 1)

if [ -z "$LAST_YIELD_LINE" ]; then
    echo "⚠️ Warning: No YIELD_CYCLE entry found in ledger. Balance unchanged."
    exit 0
fi

# 2. Extract the 'echo_earned' value using jq
# The -r flag is essential for raw output.
# The 'select(.status == "SUCCESS")' ensures only successful transactions are counted.
ECHO_EARNED=$(echo "$LAST_YIELD_LINE" | jq -r 'select(.status == "SUCCESS") | .echo_earned')

if [ -z "$ECHO_EARNED" ] || [ "$ECHO_EARNED" == "null" ]; then
    echo "⚠️ Warning: Could not extract ECHO yield from the last ledger line. Skipping."
    exit 0
fi

# 3. Read current balance (initialize if file is missing or empty)
CURRENT_BALANCE=0.0
if [ -f "$WALLET_FILE" ] && [ -s "$WALLET_FILE" ]; then
    CURRENT_BALANCE=$(cat "$WALLET_FILE")
fi

# 4. Calculate new balance (using bc for floating point arithmetic)
NEW_BALANCE=$(echo "$CURRENT_BALANCE + $ECHO_EARNED" | bc)

# 5. Write the new balance back to the file
echo "$NEW_BALANCE" > "$WALLET_FILE"

echo "✅ Wallet updated."
echo "   Last Yield: $ECHO_EARNED ECHO"
echo "   New Balance: $NEW_BALANCE ECHO"

# Note: The dashboard will read this raw float value from the echo-wallet.balance file.
