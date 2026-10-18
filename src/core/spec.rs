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

    /// Build a REST operation routing table from the spec.
    ///
    /// Returns a list of (method, path_pattern, operation_name) tuples,
    /// ordered by specificity (longest path first).
    /// The path_pattern uses `{ParamName}` for path parameters.
    pub fn build_rest_op_table(&self) -> Vec<(String, String, String)> {
        let mut entries: Vec<(String, String, String)> = Vec::new();
        for (op_name, op_data) in &self.operations {
            if let Some(http) = op_data.get("http") {
                let method = http.get("method").and_then(|m| m.as_str()).unwrap_or("GET").to_string();
                let uri = http.get("requestUri").and_then(|u| u.as_str()).unwrap_or("/").to_string();
                entries.push((method, uri, op_name.clone()));
            }
        }
        // Sort by path length (longest first) so specific paths match before general ones
        entries.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        entries
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
        // Just ensure we can call get_spec
        let spec = get_spec("nonexistent_service_xyz");
        // May or may not find it depending on environment
        let _ = spec;
    }
}
