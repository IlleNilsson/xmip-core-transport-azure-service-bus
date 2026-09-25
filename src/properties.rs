//! The `BrokerProperties` header, which is where a Service Bus message's
//! properties travel.
//!
//! Not in its body — the body is the message, bytes as they are — but in
//! one header, a JSON object naming the message id, the lock token a
//! peek-lock handed out, the sequence number. Both ends read it. The error
//! answer and the judgement that sat beside it until 2026-09-14 are the
//! namespace's, which Event Hubs shares, and moved to the http technology
//! (ADR-0044).

use serde_json::Value;
use transport::error::{Result, protocol_error};

use net::http::{Request, Response};

/// The one header a message's properties travel in.
pub const BROKER_PROPERTIES: &str = "BrokerProperties";

/// The properties a request carries in its `BrokerProperties` header, or
/// an empty object where it carries none.
///
/// # Errors
/// Where the header is present and not a JSON object.
pub fn properties_of(request: &Request) -> Result<Value> {
    parse(request.header_value(BROKER_PROPERTIES))
}

/// The properties an answer carries in its `BrokerProperties` header, or
/// an empty object where it carries none.
///
/// # Errors
/// Where the header is present and not a JSON object.
pub fn properties_in(response: &Response) -> Result<Value> {
    parse(response.header_value(BROKER_PROPERTIES))
}

fn parse(header: Option<&str>) -> Result<Value> {
    let Some(header) = header else {
        return Ok(Value::Object(serde_json::Map::new()));
    };
    let value: Value = serde_json::from_str(header)
        .map_err(|e| protocol_error(format!("BrokerProperties that are not JSON: {e}")))?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(protocol_error("BrokerProperties that are not an object"))
    }
}

/// One string property, where `properties` has it.
#[must_use]
pub fn property<'a>(properties: &'a Value, name: &str) -> Option<&'a str> {
    properties[name].as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_properties_travel_in_one_header_and_read_back_or_are_refused() {
        let request = Request::new("POST", "/q/messages")
            .header(BROKER_PROPERTIES, r#"{"MessageId":"m-1","Label":"orders"}"#);
        let properties = properties_of(&request).expect("an object");
        assert_eq!(property(&properties, "MessageId"), Some("m-1"));
        assert_eq!(property(&properties, "LockToken"), None);
        assert_eq!(
            properties_of(&Request::new("GET", "/")).expect("none"),
            Value::Object(serde_json::Map::new())
        );
        let response = Response::new(201).header(BROKER_PROPERTIES, r#"{"LockToken":"lt"}"#);
        assert_eq!(
            property(&properties_in(&response).expect("an object"), "LockToken"),
            Some("lt")
        );
        assert!(properties_of(&Request::new("GET", "/").header(BROKER_PROPERTIES, "[]")).is_err());
        assert!(properties_of(&Request::new("GET", "/").header(BROKER_PROPERTIES, "{")).is_err());
    }
}
