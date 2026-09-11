//! Xmip's side: the three calls a Location makes, each one request with a
//! Shared Access Signature over one connection to the namespace.
//!
//! A queue is a path under the namespace — `https://ns.servicebus.windows.
//! net/orders` in the cloud, `http://127.0.0.1:port/orders` for a stand-in
//! — and its messages are three calls: `POST …/messages` sends one, `POST
//! …/messages/head` peeks the next and locks it, `DELETE …/messages/<id>/
//! <lock>` completes it. A peek-lock waits up to `timeout` seconds for a
//! message where there is none, then answers 204: long polling, as the
//! service calls it.

use std::time::Duration;

use serde_json::Value;
use transport::error::{Result, protocol_error};

use crate::rest::{self, property};
use crate::sas::{self, Signer};
use http::endpoint;
use http::message::{self, Request, Response};

/// The most seconds one peek-lock waits for a message: what the service
/// allows a `timeout` to be.
pub const MAX_WAIT: u8 = 230;

/// One message as it came off the queue, locked to this receiver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Locked {
    pub id: String,
    pub lock_token: String,
    pub sequence: u64,
    pub body: Vec<u8>,
}

pub struct Client {
    endpoint: String,
    host: String,
    signer: Signer,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to the namespace at `endpoint` — `http://host:port` or
    /// `https://host:port` — signing as `policy` with `key`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, policy: &str, key: &str) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            host: endpoint::authority(endpoint)?,
            signer: Signer::new(policy, key),
            timeout: None,
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The resource a token for `queue` names: the queue's own URL.
    #[must_use]
    pub fn resource(&self, queue: &str) -> String {
        format!("{}/{queue}", self.endpoint)
    }

    /// Send `bytes` as one message to `queue`.
    ///
    /// # Errors
    /// Where the namespace refused or could not be reached.
    pub fn send(&self, queue: &str, bytes: &[u8]) -> Result<()> {
        let request = Request::new("POST", format!("/{queue}/messages"))
            .header("Content-Type", "application/octet-stream")
            .body(bytes);
        self.call(queue, request).map(|_| ())
    }

    /// The next message on `queue`, locked to this receiver until it is
    /// completed, waiting up to `wait` seconds for one where none is there;
    /// `None` where none came.
    ///
    /// # Errors
    /// Where the namespace refused, could not be reached, or answered a
    /// message with no id or lock token.
    pub fn peek_lock(&self, queue: &str, wait: u8) -> Result<Option<Locked>> {
        let request = Request::new("POST", format!("/{queue}/messages/head"))
            .query("timeout", &wait.min(MAX_WAIT).to_string());
        let answer = self.call(queue, request)?;
        if answer.status == 204 {
            return Ok(None);
        }
        let properties = rest::properties_in(&answer)?;
        let named = |name: &str| {
            property(&properties, name)
                .map(str::to_string)
                .ok_or_else(|| protocol_error(format!("a locked message with no {name}")))
        };
        Ok(Some(Locked {
            id: named("MessageId")?,
            lock_token: named("LockToken")?,
            sequence: properties["SequenceNumber"].as_u64().unwrap_or(0),
            body: answer.body,
        }))
    }

    /// Complete the message `id` on `queue`, held under `lock_token`: it is
    /// a Stream now, and leaves the queue.
    ///
    /// # Errors
    /// Where the lock has lapsed, or the namespace refused or could not be
    /// reached.
    pub fn complete(&self, queue: &str, id: &str, lock_token: &str) -> Result<()> {
        let request = Request::new("DELETE", format!("/{queue}/messages/{id}/{lock_token}"));
        self.call(queue, request).map(|_| ())
    }

    fn call(&self, queue: &str, request: Request) -> Result<Response> {
        let request = request.header("Host", &self.host);
        let expiry = sas::now() + sas::LIFETIME;
        let signed = self.signer.sign(request, &self.resource(queue), expiry);
        let stream = endpoint::connect(&self.endpoint, self.timeout)?;
        rest::judge("Service Bus", message::exchange(stream, &signed)?)
    }
}

/// The properties a send names, as the far end reads them: the message id
/// the sender chose, where it chose one.
#[must_use]
pub fn chosen_id(properties: &Value) -> Option<String> {
    property(properties, "MessageId").map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::socket;

    #[test]
    fn the_three_calls_reach_a_session_and_come_back_shaped_as_service_bus_shapes_them() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session =
                Session::new("policy", "secret").timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..6)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client = Client::new(&format!("http://{address}/"), "policy", "secret")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        client.send("orders", b"UNA:+.? '").expect("sent");
        client.send("orders", &[0, 0xff, b'\n']).expect("sent");
        let first = client
            .peek_lock("orders", 5)
            .expect("locked")
            .expect("a message");
        assert_eq!(first.body, b"UNA:+.? '");
        assert_eq!(first.sequence, 1);
        let second = client
            .peek_lock("orders", 0)
            .expect("locked")
            .expect("a message");
        assert_eq!(second.body, [0, 0xff, b'\n']);
        assert!(client.peek_lock("orders", 0).expect("none").is_none());
        client
            .complete("orders", &first.id, &first.lock_token)
            .expect("completed");
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.messages().len(), 1, "one left locked");
        assert!(matches!(
            &events[2],
            Event::Locked {
                id: Some(_),
                wait: 5,
                ..
            }
        ));
        assert!(matches!(
            &events[4],
            Event::Locked {
                id: None,
                wait: 0,
                ..
            }
        ));
        assert_eq!(
            events[5],
            Event::Completed(format!("http://{address}/orders#{}", first.id))
        );
    }

    #[test]
    fn a_lapsed_lock_and_a_missing_endpoint_are_each_refused_in_their_own_way() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("policy", "secret");
            session.serve_one(&listener).expect("served")
        });
        let client = Client::new(&format!("http://{address}"), "policy", "secret").expect("ok");
        let gone = client
            .complete("orders", "m-1", "no-such-lock")
            .expect_err("lapsed");
        assert!(gone.message.contains("404"), "{gone}");
        assert!(!gone.retryable);
        assert_eq!(
            far_end.join().expect("thread"),
            Event::Refused("40400".to_string())
        );
        let nobody = Client::new("http://127.0.0.1:1", "p", "k").expect("ok");
        assert!(nobody.send("q", b"x").expect_err("nobody").retryable);
        assert!(Client::new("ns.local", "p", "k").is_err());
        assert_eq!(
            client.resource("orders"),
            format!("http://{address}/orders")
        );
        assert_eq!(
            chosen_id(&serde_json::json!({"MessageId": "m"})),
            Some("m".to_string())
        );
    }
}
