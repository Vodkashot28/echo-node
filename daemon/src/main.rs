use std::{
    fs::{OpenOptions, read_to_string},
    io::{self, Write},
    path::PathBuf,
    thread,
    time::Duration,
};

use chrono::Utc;
use serde::Deserialize;
use serde_json;
use rand::Rng;

// NOTE: Ensure your Cargo.toml includes all necessary crates (chrono, rand, serde, serde_json, toml)

// --- Configuration Constants ---
const LEDGER_PATH_REL: &str = "echo-site/resurrection_ledger.log";
const MIN_BANDWIDTH_MB: f64 = 10.0;
const ECHO_PER_MB: f64 = 0.1; // Base rate
const ML_BONUS_MULTIPLIER: f64 = 1.15; // +15% bonus for ML contribution

// --- Resurrection Ledger Entry ---
#[derive(serde::Serialize, Debug)]
struct LedgerEntry {
    timestamp: String,
    event_type: String,
    bandwidth_shared_mb: f64,
    echo_earned: f64,
    artifact_cid: String,
    status: String,
    node_name: String,
    region: String,
    purity_score: f64,
}

// --- Configuration Structs ---
// These structures must exactly match the keys defined in Cargo.toml.
#[derive(Debug, Deserialize)]
struct Config {
    solana: SolanaConfig,
    node_identity: NodeIdentity,
    data_protocol: DataProtocol,
    ritual_cycle: RitualCycle,
    purity_stake: PurityStake,
    host_optimization: HostOptimization,
    echoname_service: EchoNameService,
}

#[derive(Debug, Deserialize)]
struct SolanaConfig {
    rpc_url: String,
    websocket_url: String,
    program_id: String,
    keypair_path: String,
}

#[derive(Debug, Deserialize)]
struct NodeIdentity {
    node_name: String,
    region: String,
    owner_x: String,
    contact_email: String,
}

#[derive(Debug, Deserialize)]
struct DataProtocol {
    ipfs_gateway: String,
    local_storage_path: String,
    max_storage_gb: u32,
    max_bandwidth_mbps: u32,
}

#[derive(Debug, Deserialize)]
struct RitualCycle {
    instruction_interval_seconds: u64,
    max_batch_size: u32,
    min_balance_for_certify_echo: f64,
}

#[derive(Debug, Deserialize)]
struct PurityStake {
    auto_stake_enabled: bool,
    surplus_stake_threshold_echo: f64,
    min_stake_amount: f64,
}

#[derive(Debug, Deserialize)]
struct HostOptimization {
    max_cpu_cores: u8,
    max_ram_gb: u32,
    priority_fee_microlamports: u64,
    enable_gpu_offload: bool,
}

#[derive(Debug, Deserialize)]
struct EchoNameService {
    program_id: String,
    min_purity_score_for_registration: f64,
}

// --- Metadata Wrapper Struct for CORRECT Parsing ---
// This is the key to reading the [package.metadata.echomesh] table.
#[derive(Debug, Deserialize)]
struct Metadata {
    package: Package,
}

#[derive(Debug, Deserialize)]
struct Package {
    metadata: Option<MetadataContent>,
}

#[derive(Debug, Deserialize)]
struct MetadataContent {
    echomesh: Config,
}


// --- Get Project Root (Helper Function) ---
fn get_project_root() -> io::Result<PathBuf> {
    let current_dir = std::env::current_dir()?;
    if current_dir.file_name().map_or(false, |name| name == "src") {
        // If in /daemon/src, return the project root (up two levels)
        Ok(current_dir.parent().unwrap().parent().unwrap().to_path_buf())
    } else if current_dir.file_name().map_or(false, |name| name == "daemon") {
        // If in /daemon, return the project root (up one level)
        Ok(current_dir.parent().unwrap().to_path_buf())
    } else {
        // Assume already at the project root
        Ok(current_dir)
    }
}

// --- Load TOML Config from Cargo.toml (CORRECTED) ---
fn load_config_from_cargo() -> Result<Config, Box<dyn std::error::Error>> {
    let root = get_project_root()?;
    let toml_path = root.join("daemon/Cargo.toml"); // Assume Cargo.toml is in the 'daemon' subdirectory

    let toml_content = read_to_string(&toml_path)?;

    // Deserialize the whole file structure into the Metadata wrapper
    let root_config: Metadata = toml::from_str(&toml_content)?;
    
    // Safely unwrap and return the nested Config data
    let config = root_config.package
        .metadata
        .ok_or("Missing [package.metadata] section in Cargo.toml")?
        .echomesh;

    Ok(config)
}

// --- Append to Ledger ---
fn append_ledger_entry(entry: &LedgerEntry) -> io::Result<()> {
    let root = get_project_root().expect("Could not determine project root.");
    let ledger_path = root.join(LEDGER_PATH_REL);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(ledger_path)?;

    let json_line = serde_json::to_string(entry)?;
    writeln!(file, "{}", json_line)?;
    Ok(())
}

// --- Main Ritual Loop ---
fn main() {
    // Attempt to load configuration from Cargo.toml
    let config = match load_config_from_cargo() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("❌ Failed to load or parse config from Cargo.toml. Error: {}", e);
            eprintln!("Ensure the required configuration tables are correctly nested under [package.metadata.echomesh] in daemon/Cargo.toml.");
            return;
        }
    };

    // Print professional initialization status and use ALL config fields to silence warnings
    println!("🔮 EchoMesh Fused Daemon Initialized (Devnet)");
    println!("--------------------------------------------------");
    println!("Node Identity: {} | Region: {}", config.node_identity.node_name, config.node_identity.region);
    println!("Node Owner X: {}", config.node_identity.owner_x);
    println!("Solana RPC Target: {}", config.solana.rpc_url); 
    println!("Solana WS Target: {}", config.solana.websocket_url); 
    println!("Artifact Storage Path: {}", config.data_protocol.local_storage_path);
    println!("Max Storage Capacity: {} GB", config.data_protocol.max_storage_gb);
    println!("Host Optimization: CPU Cores {} | RAM {} GB | GPU Offload {}", 
        config.host_optimization.max_cpu_cores, 
        config.host_optimization.max_ram_gb,
        if config.host_optimization.enable_gpu_offload { "Enabled" } else { "Disabled" });
    println!("Purity Stake Auto-Enabled: {}", config.purity_stake.auto_stake_enabled);
    println!("Ritual Cycle Interval: {} seconds", config.ritual_cycle.instruction_interval_seconds);
    println!("Domain Purity Threshold: {:.0}%", config.echoname_service.min_purity_score_for_registration * 100.0);
    println!("--------------------------------------------------");
    
    // Use remaining fields to completely silence the 'dead_code' warnings
    let _ = config.node_identity.contact_email;
    let _ = config.solana.program_id;
    let _ = config.solana.keypair_path;
    let _ = config.data_protocol.ipfs_gateway;
    let _ = config.data_protocol.max_bandwidth_mbps;
    let _ = config.ritual_cycle.max_batch_size;
    let _ = config.ritual_cycle.min_balance_for_certify_echo;
    let _ = config.purity_stake.surplus_stake_threshold_echo;
    let _ = config.purity_stake.min_stake_amount;
    let _ = config.host_optimization.priority_fee_microlamports;
    let _ = config.echoname_service.program_id;


    let mut rng = rand::thread_rng();
    let interval = config.ritual_cycle.instruction_interval_seconds;
    let max_bandwidth = config.data_protocol.max_bandwidth_mbps as f64;
    
    // Apply the 15% yield bonus for ML contribution, and explicitly use f64
    let effective_echo_per_mb: f64 = ECHO_PER_MB * ML_BONUS_MULTIPLIER; 

    loop {
        // Bandwidth simulation uses the max_bandwidth_mbps from config
        let bandwidth_shared: f64 = rng.gen_range(MIN_BANDWIDTH_MB..=max_bandwidth);
        
        // Calculate yield with the 15% ML bonus, rounded to two decimal places
        let echo_earned: f64 = (bandwidth_shared * effective_echo_per_mb * 100.0).round() / 100.0;
        
        // --- Simulation Hook: Purity Score ---
        let purity_score: f64 = 0.92_f64 + (rng.gen_range(-0.01..=0.01));
        let purity_score: f64 = purity_score.clamp(0.0_f64, 1.0_f64);


        let entry = LedgerEntry {
            timestamp: Utc::now().to_rfc3339(),
            event_type: "YIELD_CYCLE".to_string(),
            bandwidth_shared_mb: bandwidth_shared,
            echo_earned,
            artifact_cid: "simulated_cid_0x0001".to_string(),
            status: "SUCCESS".to_string(),
            node_name: config.node_identity.node_name.clone(),
            region: config.node_identity.region.clone(),
            purity_score,
        };

        println!(
            "[{}] Ritual: {:.2} MB → {:.2} ECHO (w/ML+) | Purity: {:.1}%",
            Utc::now().format("%H:%M:%S"),
            bandwidth_shared,
            echo_earned,
            purity_score * 100.0
        );

        // Conditional check for EchoName Service eligibility
        if purity_score >= config.echoname_service.min_purity_score_for_registration {
            println!("  ✅ Domain Eligible: Node purity exceeds {:.0}%", config.echoname_service.min_purity_score_for_registration * 100.0);
        } else {
            println!("  ⚠️ Purity Warning: Below registration threshold.");
        }

        // Append entry to the Resurrection Ledger
        match append_ledger_entry(&entry) {
            Ok(_) => println!("  📜 Ledger updated."),
            Err(e) => eprintln!("  ❌ Ledger error: {}", e),
        }

        // Wait for the configured interval
        thread::sleep(Duration::from_secs(interval));
    }
}
