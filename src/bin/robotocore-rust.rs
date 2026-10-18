//! Robotocore Rust binary - AWS API mock server.
//!
//! Runs an HTTP server on a configurable port that mocks AWS API responses.

use clap::Parser;
use tokio::net::TcpListener;

use robotocore_rust::core::{build_router, ServiceRegistry};

/// Robotocore Rust - AWS API Mock Server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "4567")]
    port: u16,

    /// AWS account ID (12-digit number)
    #[arg(short, long, default_value = "123456789012")]
    account: String,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// URL of the Moto sidecar (e.g., http://127.0.0.1:4568)
    #[arg(long)]
    moto_url: Option<String>,

    /// Port for the Moto sidecar
    #[arg(long, default_value = "4568")]
    moto_port: u16,

    /// Start the Moto sidecar automatically
    #[arg(long)]
    auto_moto: bool,
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
    let registry = ServiceRegistry::new();

    // Optionally start Moto proxy
    let native_list = vec![
        "sts".to_string(),
        "sqs".to_string(),
        "s3".to_string(),
        "dynamodb".to_string(),
        "sns".to_string(),
        "secretsmanager".to_string(),
        "kms".to_string(),
        "ssm".to_string(),
        "iam".to_string(),
        "lambda".to_string(),
        "logs".to_string(),
        "events".to_string(),
        "kinesis".to_string(),
        "firehose".to_string(),
        "cloudwatch".to_string(),
        "ecr".to_string(),
        "ecs".to_string(),
        "stepfunctions".to_string(),
    ];
    let moto_proxy = match &args.moto_url {
        Some(url) => Some(robotocore_rust::core::proxy::MotoProxy::new(
            url.clone(),
            native_list.clone(),
        )),
        None if args.auto_moto => {
            let url = format!("http://127.0.0.1:{}", args.moto_port);
            Some(robotocore_rust::core::proxy::MotoProxy::new(
                url,
                native_list,
            ))
        }
        _ => None,
    };

    if moto_proxy.is_some() {
        tracing::info!("Moto proxy enabled for non-native services");
    }

    // Build Axum app
    let app = build_router(registry, moto_proxy);

    // Create listener
    let addr = format!("127.0.0.1:{}", args.port);
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
