//! AWS service specification loader.
//!
//! Loads and caches AWS service specs (service-2.json files) from botocore.
//! Provides parsed access to operations, shapes, and metadata.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// AWS service specification metadata and operations.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    pub protocol: String,
    pub service_name: String,
    pub operations: HashMap<String, Value>,
    pub shapes: HashMap<String, Value>,
    pub metadata: Value,
}

impl ServiceSpec {
    /// Load a service spec from a service-2.json file.
    pub fn load_from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let spec_json: Value = serde_json::from_str(&content)?;

        let protocol = spec_json
            .get("metadata")
            .and_then(|m| m.get("protocol"))
            .and_then(|p| p.as_str())
            .unwrap_or("query")
            .to_string();

        let service_name = spec_json
            .get("metadata")
            .and_then(|m| m.get("serviceAbbreviation"))
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();

        let operations = spec_json
            .get("operations")
            .and_then(|o| o.as_object())
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let shapes = spec_json
            .get("shapes")
            .and_then(|s| s.as_object())
            .map(|s| s.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        Ok(ServiceSpec {
            protocol,
            service_name,
            operations,
            shapes,
            metadata: spec_json
                .get("metadata")
                .cloned()
                .unwrap_or(Value::Object(Default::default())),
        })
    }

    /// Get an operation by name.
    pub fn get_operation(&self, name: &str) -> Option<&Value> {
        self.operations.get(name)
    }

    /// Get a shape by name.
    pub fn get_shape(&self, name: &str) -> Option<&Value> {
        self.shapes.get(name)
    }
}

/// Global spec cache: service name -> ServiceSpec
static SPEC_CACHE: Lazy<RwLock<HashMap<String, ServiceSpec>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Load or retrieve a cached service spec.
pub fn get_spec(service: &str) -> Option<ServiceSpec> {
    // Check cache
    {
        let cache = SPEC_CACHE.read();
        if let Some(spec) = cache.get(service) {
            return Some(spec.clone());
        }
    }

    // Try to load from botocore data directory
    let specs_dir = std::env::var("ROBOTOCORE_SPECS_DIR").unwrap_or_else(|_| {
        // Default to botocore installation
        "/opt/homebrew/lib/python3.14/site-packages/botocore/data".to_string()
    });

    // Try to find the latest version of the service spec
    let service_path = PathBuf::from(&specs_dir).join(service);
    if service_path.exists() {
        // Find the version directory (usually like "2011-06-15")
        if let Ok(entries) = fs::read_dir(&service_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let spec_file = path.join("service-2.json.gz");
                    if spec_file.exists() {
                        // Decompress and load
                        if let Ok(spec) = load_gzip_spec(&spec_file) {
                            let mut cache = SPEC_CACHE.write();
                            cache.insert(service.to_string(), spec.clone());
                            return Some(spec);
                        }
                    }
                }
            }
        }
    }

    None
}

fn load_gzip_spec(path: &PathBuf) -> Result<ServiceSpec, Box<dyn std::error::Error>> {
    use std::io::Read;

    let file = fs::File::open(path)?;
    let mut decoder = flate2::read::GzDecoder::new(file);
    let mut content = String::new();
    decoder.read_to_string(&mut content)?;

    let spec_json: Value = serde_json::from_str(&content)?;

    let protocol = spec_json
        .get("metadata")
        .and_then(|m| m.get("protocol"))
        .and_then(|p| p.as_str())
        .unwrap_or("query")
        .to_string();

    let service_name = spec_json
        .get("metadata")
        .and_then(|m| m.get("serviceAbbreviation"))
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let operations = spec_json
        .get("operations")
        .and_then(|o| o.as_object())
        .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    let shapes = spec_json
        .get("shapes")
        .and_then(|s| s.as_object())
        .map(|s| s.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    Ok(ServiceSpec {
        protocol,
        service_name,
        operations,
        shapes,
        metadata: spec_json
            .get("metadata")
            .cloned()
            .unwrap_or(Value::Object(Default::default())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec_cache_empty() {
        // get_spec on a name that cannot exist returns None without panicking
        assert!(get_spec("nonexistent_service_xyz_cache_probe").is_none());
    }

    #[test]
    fn test_service_spec_lookup() {
        let mut operations = HashMap::new();
        operations.insert(
            "ListThings".to_string(),
            serde_json::json!({"name": "ListThings", "http": {"method": "POST", "requestUri": "/"}}),
        );
        let mut shapes = HashMap::new();
        shapes.insert(
            "Thing".to_string(),
            serde_json::json!({"type": "structure"}),
        );
        let spec = ServiceSpec {
            protocol: "json".to_string(),
            service_name: "Test".to_string(),
            operations,
            shapes,
            metadata: serde_json::json!({"protocol": "json"}),
        };
        assert!(spec.get_operation("ListThings").is_some());
        assert!(spec.get_operation("Missing").is_none());
        assert!(spec.get_shape("Thing").is_some());
        assert_eq!(spec.protocol, "json");
    }
}
