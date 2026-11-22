//! Authentication — JWT token generation and validation.
//!
//! The JWT code needs the feature `auth` (default). Without it, [`AuthConfig`] still
//! exists, so the API is the same, but authentication fails closed: an enabled
//! config is refused by [`AuthConfig::validate`], no token is issued or accepted,
//! and every protected request is unauthorized.

use anyhow::Result;
#[cfg(feature = "auth")]
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
#[cfg(feature = "auth")]
use serde::{Deserialize, Serialize};

/// Why authentication cannot work in a build without the feature `auth`.
#[cfg(not(feature = "auth"))]
const NOT_COMPILED_IN: &str =
    "authentication is not available: snapcast-control was built without the feature `auth`";

/// JWT claims.
#[cfg(feature = "auth")]
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    /// Subject (client ID or username).
    sub: String,
    /// Expiration (Unix timestamp).
    exp: u64,
}

/// Auth configuration.
///
/// `Default` is disabled with an **empty** secret. A real secret must be
/// supplied explicitly (config/CLI) before authentication is enabled — there
/// is deliberately no built-in default secret, since a shipped constant would
/// be public and forgeable by anyone.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Whether authentication is required.
    pub enabled: bool,
    /// Secret key for JWT signing/validation.
    pub secret: String,
}

impl AuthConfig {
    /// Reject an inconsistent configuration (enabled without a secret, or enabled
    /// in a build without the feature `auth`).
    ///
    /// Call this at startup and refuse to run on error — an enabled-but-
    /// secretless config would otherwise sign tokens with an empty key.
    pub fn validate(&self) -> Result<()> {
        #[cfg(not(feature = "auth"))]
        if self.enabled {
            anyhow::bail!("{NOT_COMPILED_IN}");
        }
        if self.enabled && self.secret.trim().is_empty() {
            anyhow::bail!(
                "authentication is enabled but no secret is configured \
                 (set [auth] secret in the config file or pass --auth-secret)"
            );
        }
        Ok(())
    }
}

/// Validate an HTTP Authorization header. Returns Ok(subject) or Err.
pub fn validate_bearer(config: &AuthConfig, header: Option<&str>) -> Result<String> {
    if !config.enabled {
        return Ok("anonymous".into());
    }
    let header = header.ok_or_else(|| anyhow::anyhow!("missing Authorization header"))?;
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| anyhow::anyhow!("expected Bearer token"))?;
    validate_token(config, token)
}

/// Generate a JWT token for the given subject.
#[cfg(feature = "auth")]
pub fn generate_token(config: &AuthConfig, subject: &str) -> Result<String> {
    if config.secret.is_empty() {
        anyhow::bail!("cannot issue token: auth secret is not configured");
    }
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        + 86400; // 24 hours

    let claims = Claims {
        sub: subject.into(),
        exp,
    };

    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(config.secret.as_bytes()),
    )?;
    tracing::info!(subject, "auth token generated");
    Ok(token)
}

/// Generate a JWT token: always an error without the feature `auth`.
#[cfg(not(feature = "auth"))]
pub fn generate_token(_config: &AuthConfig, _subject: &str) -> Result<String> {
    anyhow::bail!("{NOT_COMPILED_IN}")
}

/// Validate a JWT token: always an error without the feature `auth`, so an
/// enabled config lets nobody in.
#[cfg(not(feature = "auth"))]
pub fn validate_token(_config: &AuthConfig, _token: &str) -> Result<String> {
    anyhow::bail!("{NOT_COMPILED_IN}")
}

/// Validate a JWT token. Returns the subject if valid.
#[cfg(feature = "auth")]
pub fn validate_token(config: &AuthConfig, token: &str) -> Result<String> {
    if config.secret.is_empty() {
        anyhow::bail!("cannot validate token: auth secret is not configured");
    }
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(config.secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|e| {
        tracing::warn!(error = %e, "auth token validation failed");
        e
    })?;
    Ok(data.claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "auth")]
    #[test]
    fn token_roundtrip() {
        let config = AuthConfig {
            enabled: true,
            secret: "test-secret-must-be-32-bytes-long".into(),
        };
        let token = generate_token(&config, "user1").unwrap();
        let subject = validate_token(&config, &token).unwrap();
        assert_eq!(subject, "user1");
    }

    #[cfg(feature = "auth")]
    #[test]
    fn invalid_token() {
        let config = AuthConfig {
            enabled: true,
            secret: "test-secret-must-be-32-bytes-long".into(),
        };
        assert!(validate_token(&config, "garbage").is_err());
    }

    #[cfg(feature = "auth")]
    #[test]
    fn wrong_secret() {
        let config1 = AuthConfig {
            enabled: true,
            secret: "secret1-must-be-at-least-32bytes!".into(),
        };
        let config2 = AuthConfig {
            enabled: true,
            secret: "secret2-must-be-at-least-32bytes!".into(),
        };
        let token = generate_token(&config1, "user1").unwrap();
        assert!(validate_token(&config2, &token).is_err());
    }

    #[test]
    fn default_is_disabled_with_empty_secret() {
        let config = AuthConfig::default();
        assert!(!config.enabled);
        assert!(config.secret.is_empty(), "must not ship a built-in secret");
        // A disabled config is internally consistent.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_enabled_without_secret() {
        let config = AuthConfig {
            enabled: true,
            secret: "   ".into(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn token_ops_refuse_empty_secret() {
        let config = AuthConfig {
            enabled: true,
            secret: String::new(),
        };
        assert!(generate_token(&config, "user1").is_err());
        assert!(validate_token(&config, "any.token.value").is_err());
    }

    /// Without the feature, an enabled config fails closed everywhere.
    #[cfg(not(feature = "auth"))]
    #[test]
    fn without_the_feature_auth_fails_closed() {
        let config = AuthConfig {
            enabled: true,
            secret: "test-secret-must-be-32-bytes-long".into(),
        };
        assert!(config.validate().is_err());
        assert!(generate_token(&config, "user1").is_err());
        assert!(validate_token(&config, "any.token.value").is_err());
        assert!(validate_bearer(&config, Some("Bearer any.token.value")).is_err());
        // Disabled stays open, as with the feature.
        assert!(AuthConfig::default().validate().is_ok());
        assert!(validate_bearer(&AuthConfig::default(), None).is_ok());
    }
}
