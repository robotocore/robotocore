//! Account and region identification for AWS requests.
//!
//! AWS requests are scoped to (account_id, region) pairs. This module provides:
//! - Account extraction from access key ID (12-digit = account ID, else default)
//! - Account/region keying for state isolation

/// Account and region identifier.
///
/// Used as a key for scoping resources, state, and audit logs to specific
/// AWS account + region combinations.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AccountRegion {
    pub account: u64,
    pub region: String,
}

impl AccountRegion {
    /// Create a new AccountRegion.
    pub fn new(account: u64, region: String) -> Self {
        Self { account, region }
    }

    /// Parse account ID from access key ID.
    ///
    /// If access_key_id is exactly 12 digits, it is treated as the account ID.
    /// Otherwise, returns the default account (123456789012).
    pub fn from_access_key(access_key_id: &str) -> Self {
        let account = parse_account_from_key(access_key_id);
        Self {
            account,
            region: "us-east-1".to_string(),
        }
    }
}

/// Extract account ID from an access key ID string.
///
/// - If the string is exactly 12 digits, parse as account ID.
/// - Otherwise, return the default account (123456789012).
pub fn parse_account_from_key(access_key_id: &str) -> u64 {
    if access_key_id.len() == 12 && access_key_id.chars().all(|c| c.is_ascii_digit()) {
        access_key_id.parse().unwrap_or(123456789012)
    } else {
        123456789012
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_12_digit_key() {
        let account = parse_account_from_key("123456789012");
        assert_eq!(account, 123456789012);
    }

    #[test]
    fn test_parse_non_digit_key() {
        let account = parse_account_from_key("AKIAIOSFODNN7EXAMPLE");
        assert_eq!(account, 123456789012);
    }

    #[test]
    fn test_parse_short_key() {
        let account = parse_account_from_key("test");
        assert_eq!(account, 123456789012);
    }

    #[test]
    fn test_account_region_creation() {
        let ar = AccountRegion::new(999999999999, "eu-west-1".to_string());
        assert_eq!(ar.account, 999999999999);
        assert_eq!(ar.region, "eu-west-1");
    }
}
