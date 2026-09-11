//! The Shared Access Signature every `servicebus.windows.net` namespace
//! takes: what a request carries to prove who sent it.
//!
//! A token names the resource it is good for, when it stops being good and
//! which policy's key made it —
//! `SharedAccessSignature sr=<resource>&sig=<signature>&se=<expiry>&skn=<policy>`
//! — and the signature is HMAC-SHA256 under the key of the resource,
//! percent-encoded, a line feed, and the expiry in seconds since the epoch;
//! base64, then percent-encoded in its turn. The key is the policy's key as
//! the portal shows it, taken as the bytes of that text. Both sides are
//! here — a Location signs, [`crate::Session`] verifies — because
//! verifying is the same computation with a comparison at the end, and
//! Event Hubs signs the same way at the same namespaces, so that
//! technology takes this file rather than writing it again (ADR-0044).
//!
//! **The body is not signed**, nor the request: a token is good for every
//! request at its resource until it expires. The `tls` feature is what
//! keeps a token and the message it carries from being read on the way.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use transport::error::{Result, protocol_error};

use http::message::Request;
use http::percent::{decode, encode};

/// How long a token a Location makes stays good: five minutes, long enough
/// for a request and short enough that a token read in transit is soon
/// worth nothing.
pub const LIFETIME: u64 = 300;

/// The policy and key a signature is made under.
#[derive(Clone, Debug)]
pub struct Signer {
    policy: String,
    key: Vec<u8>,
}

/// What a token said, read back by the far end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    /// The resource the token is good for, decoded:
    /// `https://ns.servicebus.windows.net/orders`.
    pub resource: String,
    /// When it stops being good, seconds since the epoch.
    pub expiry: u64,
    /// The policy whose key made it.
    pub policy: String,
    signature: String,
}

impl Signer {
    /// Sign as `policy` — `RootManageSharedAccessKey`, say — with its key as
    /// the portal shows it.
    #[must_use]
    pub fn new(policy: &str, key: &str) -> Self {
        Self {
            policy: policy.to_string(),
            key: key.as_bytes().to_vec(),
        }
    }

    /// The `Authorization` value for `resource` until `expiry`.
    #[must_use]
    pub fn token(&self, resource: &str, expiry: u64) -> String {
        let resource = encode(resource, false);
        let signature = self.signature(&resource, expiry);
        format!(
            "SharedAccessSignature sr={resource}&sig={}&se={expiry}&skn={}",
            encode(&signature, false),
            self.policy
        )
    }

    /// `request` carrying a token for `resource` good until `expiry`.
    #[must_use]
    pub fn sign(&self, request: Request, resource: &str, expiry: u64) -> Request {
        request.header("Authorization", &self.token(resource, expiry))
    }

    /// Whether `request` carries a token this signer would have made that
    /// is still good at `now` and covers the path the request is for.
    ///
    /// # Errors
    /// Where the request has no token, one that is malformed, one another
    /// policy made, one that has expired, one for another resource, or one
    /// whose signature differs — each with Service Bus's own subcode.
    pub fn verify(&self, request: &Request, now: u64) -> Result<Token> {
        let token = parse(
            request
                .header_value("authorization")
                .ok_or_else(|| protocol_error("40105: Missing authorization token"))?,
        )?;
        if token.policy != self.policy {
            return Err(protocol_error(
                "40103: Invalid authorization token signature",
            ));
        }
        if token.expiry <= now {
            return Err(protocol_error("40102: Expired authorization token"));
        }
        let path = token
            .resource
            .split_once("://")
            .and_then(|(_, rest)| rest.find('/').map(|at| &rest[at..]))
            .unwrap_or("/");
        if !request.path.starts_with(path) {
            return Err(protocol_error(
                "40103: Invalid authorization token signature — another resource",
            ));
        }
        let expected = self.signature(&encode(&token.resource, false), token.expiry);
        if same(&expected, &token.signature) {
            Ok(token)
        } else {
            Err(protocol_error(
                "40103: Invalid authorization token signature",
            ))
        }
    }

    fn signature(&self, encoded_resource: &str, expiry: u64) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC takes a key of any length");
        mac.update(format!("{encoded_resource}\n{expiry}").as_bytes());
        STANDARD.encode(mac.finalize().into_bytes())
    }
}

/// What an `Authorization` value says, or why it is not a token.
///
/// # Errors
/// Where the value is not a `SharedAccessSignature` with its four parts.
pub fn parse(authorization: &str) -> Result<Token> {
    let malformed = || protocol_error("40104: Malformed authorization token");
    let fields = authorization
        .strip_prefix("SharedAccessSignature ")
        .ok_or_else(malformed)?;
    let mut token = Token {
        resource: String::new(),
        expiry: 0,
        policy: String::new(),
        signature: String::new(),
    };
    for pair in fields.split('&') {
        let (name, value) = pair.split_once('=').ok_or_else(malformed)?;
        match name {
            "sr" => token.resource = decode(value),
            "sig" => token.signature = decode(value),
            "se" => token.expiry = value.parse().map_err(|_| malformed())?,
            "skn" => token.policy = value.to_string(),
            _ => return Err(malformed()),
        }
    }
    if token.resource.is_empty() || token.signature.is_empty() || token.policy.is_empty() {
        return Err(malformed());
    }
    Ok(token)
}

/// The moment now, in seconds since the epoch, which is how `se` counts.
#[must_use]
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

/// Equal, without the comparison's timing saying how far the two agreed.
fn same(expected: &str, given: &str) -> bool {
    expected.len() == given.len()
        && expected
            .bytes()
            .zip(given.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESOURCE: &str = "http://ns.servicebus.windows.net/orders";

    #[test]
    fn a_token_is_written_as_the_namespace_wants_it_and_reads_back() {
        let signer = Signer::new("RootManageSharedAccessKey", "secret");
        let token = signer.token(RESOURCE, 1_800_000_000);
        assert!(token.starts_with(
            "SharedAccessSignature sr=http%3A%2F%2Fns.servicebus.windows.net%2Forders&sig="
        ));
        assert!(token.ends_with("&se=1800000000&skn=RootManageSharedAccessKey"));
        let read = parse(&token).expect("a token");
        assert_eq!(read.resource, RESOURCE);
        assert_eq!(read.expiry, 1_800_000_000);
        assert_eq!(read.policy, "RootManageSharedAccessKey");
        assert!(!read.signature.contains('%'), "decoded");
        let carrying = signer.sign(
            Request::new("POST", "/orders/messages"),
            RESOURCE,
            1_800_000_000,
        );
        assert_eq!(carrying.header_value("authorization"), Some(token.as_str()));
        assert_eq!(signer.verify(&carrying, 1_799_999_999).expect("good"), read);
    }

    #[test]
    fn a_wrong_key_an_expired_token_and_another_resource_are_each_refused_by_subcode() {
        let signer = Signer::new("policy", "secret");
        let request = |at: u64, resource: &str, key: &str| {
            Signer::new("policy", key).sign(Request::new("POST", "/orders/messages"), resource, at)
        };
        let refused =
            |request: &Request| signer.verify(request, 1_000).expect_err("refused").message;
        assert!(refused(&request(2_000, RESOURCE, "wrong")).starts_with("40103"));
        assert!(refused(&request(1_000, RESOURCE, "secret")).starts_with("40102"));
        let elsewhere = "http://ns.servicebus.windows.net/invoices";
        assert!(refused(&request(2_000, elsewhere, "secret")).contains("another resource"));
        let other = Signer::new("other", "secret").sign(Request::new("POST", "/"), RESOURCE, 2_000);
        assert!(refused(&other).starts_with("40103"));
        assert!(refused(&Request::new("POST", "/")).starts_with("40105"));
        let bare = Request::new("POST", "/").header("Authorization", "Bearer x");
        assert!(refused(&bare).starts_with("40104"));
        assert!(parse("SharedAccessSignature sr=a&sig=b&se=soon&skn=p").is_err());
        assert!(parse("SharedAccessSignature sr=a&sig=b&se=1").is_err());
        assert!(now() > 1_700_000_000);
    }
}
