//! The Service Bus REST API's shapes that both ends and Event Hubs share:
//! the `BrokerProperties` header, the error answer, and the judgement of
//! an answer.
//!
//! A message's properties do not travel in its body — the body is the
//! message, bytes as they are — but in one header, `BrokerProperties`, a
//! JSON object naming the message id, the lock token a peek-lock handed
//! out, the sequence number. An error is an XML `Error` with a `Code` that
//! repeats the status and a `Detail` that says what went wrong, its
//! leading subcode the part worth reading — `40103` is a bad signature.
//! Event Hubs answers the same shapes at the same namespaces, and takes
//! this file (ADR-0044).

use serde_json::Value;
use transport::error::{Result, TransportError, protocol_error};
use transport::xml::{escape, first};

use http::message::{Request, Response};

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

/// The far end's answer that is not a result: an `Error` with `status` for
/// its code and `detail` for what went wrong, `40103: Invalid authorization
/// token signature` style.
#[must_use]
pub fn error(status: u16, detail: &str) -> Response {
    let body = format!(
        "<Error><Code>{status}</Code><Detail>{}</Detail></Error>",
        escape(detail)
    );
    Response::new(status)
        .header("Content-Type", "application/xml; charset=utf-8")
        .body(body.as_bytes())
}

/// The subcode an error's detail opens with — `40103` — or the whole detail
/// where it opens with none.
#[must_use]
pub fn subcode(detail: &str) -> String {
    detail
        .split_once(':')
        .map_or(detail, |(code, _)| code)
        .trim()
        .to_string()
}

/// A 2xx answer as it is; anything else as a failure naming the status and
/// the detail the service put in the body, retryable where it says come
/// back — a server fault, a timeout, a throttle, a namespace that is busy.
///
/// # Errors
/// Where the status is not 2xx.
pub fn judge(service: &str, response: Response) -> Result<Response> {
    if (200..300).contains(&response.status) {
        return Ok(response);
    }
    let detail = first(&response.text(), "Detail").unwrap_or_default();
    let retryable = response.status >= 500
        || response.status == 408
        || response.status == 429
        || detail.starts_with("50002");
    Err(TransportError {
        message: format!("{service} answered {} {detail}", response.status),
        retryable,
    })
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

    #[test]
    fn a_server_failure_is_worth_repeating_and_a_client_one_is_not() {
        assert!(
            judge("Service Bus", Response::new(503))
                .expect_err("s")
                .retryable
        );
        assert!(
            judge("Service Bus", Response::new(429))
                .expect_err("t")
                .retryable
        );
        let busy = error(500, "50002: The server is busy <now>");
        assert_eq!(
            busy.header_value("content-type"),
            Some("application/xml; charset=utf-8")
        );
        assert!(busy.text().contains("busy &lt;now&gt;"));
        assert!(judge("Service Bus", busy).expect_err("busy").retryable);
        let denied = judge(
            "Event Hubs",
            error(401, "40103: Invalid authorization token signature"),
        )
        .expect_err("denied");
        assert!(!denied.retryable);
        assert_eq!(
            denied.message,
            "Event Hubs answered 401 40103: Invalid authorization token signature"
        );
        assert_eq!(subcode("40103: Invalid"), "40103");
        assert_eq!(subcode("no such entity"), "no such entity");
        assert_eq!(judge("x", Response::new(204)).expect("ok").status, 204);
    }
}
