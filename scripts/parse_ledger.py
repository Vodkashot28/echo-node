#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parse_ledger.py - A Ritual Helper script to parse the Resurrection Ledger
and generate a summary of the Echonomic Model's value flow.
"""

import os
import json
from pathlib import Path

# --- Configuration ---
# Path to the Resurrection Ledger relative to the project root
LEDGER_PATH_REL = Path("echo-site") / "resurrection_ledger.log"

def get_project_root() -> Path:
    """Dynamically determine the project root directory."""
    # This assumes the script is run from the project root or the 'scripts' folder
    script_path = Path(__file__).resolve()
    if script_path.parent.name == "scripts":
        return script_path.parent.parent
    return script_path.parent

def parse_ledger():
    """Reads the ledger, calculates totals, and prints a summary."""
    root = get_project_root()
    ledger_path = root / LEDGER_PATH_REL

    total_echo_earned = 0.0
    total_bandwidth_shared_mb = 0.0
    entry_count = 0

    print("--- Resurrection Ledger Analysis ---")
    print(f"Reading Ledger from: {ledger_path}")

    if not ledger_path.exists():
        print("❌ Error: Ledger file not found. Daemon may not have run yet.")
        return

    try:
        with open(ledger_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                try:
                    # Each line is a single JSON object
                    entry = json.loads(line)
                    
                    # Accumulate statistics from the entry
                    total_echo_earned += entry.get("echo_earned", 0.0)
                    total_bandwidth_shared_mb += entry.get("bandwidth_shared_mb", 0.0)
                    entry_count += 1
                
                except json.JSONDecodeError:
                    print(f"⚠️ Warning: Could not parse JSON on line {line_number}. Skipping.")
                except TypeError as e:
                    print(f"⚠️ Warning: Data type error on line {line_number}: {e}. Skipping.")

    except IOError as e:
        print(f"❌ Error reading file: {e}")
        return

    # --- Print Summary Report ---
    print("\n--- Echonomic Model Summary ---")
    print(f"📄 Total Ledger Entries: {entry_count}")
    print(f"🌐 Total Bandwidth Shared: {total_bandwidth_shared_mb:,.2f} MB")
    print(f"💸 Total ECHO Tokens Earned: {total_echo_earned:,.2f} ECHO")
    print("-------------------------------\n")

    if entry_count > 0:
        avg_echo_per_mb = total_echo_earned / total_bandwidth_shared_mb if total_bandwidth_shared_mb > 0 else 0
        print(f"Average Yield Rate: {avg_echo_per_mb:.4f} ECHO/MB")

if __name__ == "__main__":
    parse_ledger()

