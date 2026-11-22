//! validated cross-origin policy shared by the controller and HTTP workers.
use super::hostname_matches;
use http::uri::Authority;
use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;

/// allowed origin or origin pattern, with a normalized host and effective port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Origin {
    /// any serialized origin, including the opaque origin.
    Any,
    /// exact or wildcard host restricted to a scheme and port.
    Host {
        /// whether the scheme is HTTPS.
        https: bool,
        /// lowercase DNS pattern, IPv4 address, or canonical IPv6 literal.
        host: String,
        /// explicit or scheme-default port.
        port: u16,
    },
}

impl Origin {
    /// parse a serialized HTTP origin or a Gateway API wildcard pattern.
    pub fn parse(value: &str) -> Option<Self> {
        if value == "*" {
            return Some(Self::Any);
        }
        let (scheme, authority) = value.split_once("://")?;
        let https = match scheme {
            "http" => false,
            "https" => true,
            _ => return None,
        };
        let authority: Authority = authority.parse().ok()?;
        let host = authority.host();
        let port = match authority.as_str().strip_prefix(host)? {
            "" if https => 443,
            "" => 80,
            tail => {
                let digits = tail.strip_prefix(':')?;
                if digits.len() > 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                digits.parse::<u16>().ok().filter(|p| *p > 0)?
            }
        };
        let host = if let Some(ip) =
            host.strip_prefix('[').and_then(|ip| ip.strip_suffix(']'))
        {
            format!("[{}]", ip.parse::<Ipv6Addr>().ok()?)
        } else {
            let name = host.strip_prefix("*.").unwrap_or(host);
            if host != "*"
                && !name.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
            {
                return None;
            }
            host.to_ascii_lowercase()
        };
        Some(Self::Host { https, host, port })
    }

    /// compare parsed origins, including greedy label wildcards with an exact port.
    pub fn matches(&self, origin: &Self) -> bool {
        match (self, origin) {
            (Self::Any, _) => true,
            (
                Self::Host { https, host, port },
                Self::Host { https: scheme, host: name, port: number },
            ) => {
                https == scheme
                    && port == number
                    && (host == "*" || hostname_matches(host, name))
            }
            _ => false,
        }
    }
}

/// resolved fields of a rule or backend reference CORS filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cors {
    /// accepted origins with effective ports.
    pub origins: Vec<Origin>,
    /// case-sensitive methods or a single wildcard.
    pub methods: Vec<String>,
    /// allowed request header names or a single wildcard.
    pub headers: Vec<String>,
    /// response headers exposed to browser scripts.
    pub expose: Vec<String>,
    /// allow the browser to send credentials.
    pub credentials: bool,
    /// preflight cache lifetime in seconds.
    pub max_age: u32,
}
