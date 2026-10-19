//! Robotocore Rust binary - AWS API mock server.
//!
//! Runs an HTTP server on a configurable port that mocks AWS API responses.

use clap::Parser;
use tokio::net::TcpListener;

use robotocore_rust::core::state::SnapshotRegistry;
use robotocore_rust::core::{build_router, ServiceRegistry};

/// Robotocore Rust - AWS API Mock Server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "4566")]
    port: u16,

    /// AWS account ID (12-digit number)
    #[arg(short, long, default_value = "123456789012")]
    account: String,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Address to listen on (default 127.0.0.1; containers pass 0.0.0.0)
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(args.log_level.parse().unwrap_or(tracing::Level::INFO))
        .init();

    tracing::info!("Starting Robotocore Rust on port {}", args.port);
    tracing::info!("Default account: {}", args.account);

    // Create service registry
    let (registry, native_handlers) = ServiceRegistry::new_with_handlers();
    let snapshots = std::sync::Arc::new(SnapshotRegistry::new(&native_handlers));

    // Build the Axum app — every serviced request is answered by the
    // gateway core or rotocore; there is no legacy proxy plumbing.
    let app = build_router(registry, snapshots);

    // Create listener
    let addr = format!("{}:{}", args.host, args.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind to {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    tracing::info!("Server listening on {}", addr);

    // Run server
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("Server error: {}", e);
        std::process::exit(1);
    }
}
