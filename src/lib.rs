#![forbid(unsafe_code)]

//! Streams that arrive as messages on a Service Bus queue. One message is
//! one Stream, its id kept beside it.
//!
//! Service Bus is the queue of every organisation that lives in Azure, and
//! its REST API is three calls on a queue under the namespace: send a
//! message, peek-lock the next one with long polling, complete it by its
//! lock token. A Receive Location peek-locks, hands each body on as a
//! Stream and completes it once it is; a Send Location sends a Stream as
//! one message. Both carry a Shared Access Signature over plain HTTP/1.1
//! on a socket — `https://` with the `tls` feature, which is the http
//! technology's TLS (ADR-0033).
//!
//! ```text
//! properties.rs  the BrokerProperties header, both ways
//! client.rs      Xmip's side: send, peek-lock, complete
//! session.rs     the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, the percent-encoding, HTTP itself, the Shared Access
//! Signature and the namespace's error and judgement come from the http
//! technology, the flat XML scan from the capability (ADR-0044). The
//! signature and the error lived here until 2026-09-14, imported sideways
//! by azure-event-hubs; what two technologies both speak over HTTP is the
//! carrier's to share.
//!
//! A message is bytes — the body as it is, 256 KiB at most on the
//! Standard tier every namespace starts at: [`ceiling`]. Nothing is
//! refused for its content, an empty body included.
//!
//! A queue is not an artefact anyone claims: a locked message is the
//! queue's own claim until it is completed, so [`Transport::claims`]
//! answers `None`. The origin URI is the queue URL with the message id as
//! its fragment. A send target is a queue name under this transport's
//! namespace, or empty for its own queue.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up at the endpoint's authority and takes the one send.

pub mod client;
pub mod properties;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Locked, MAX_WAIT};
use http::endpoint;
pub use session::{Event, Session};
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

/// The largest message a Standard namespace carries: 256 KiB.
#[must_use]
pub const fn ceiling() -> usize {
    256 * 1024
}

/// The most messages one receive hands back.
pub const MAX_MESSAGES: usize = 10;

/// What the loopback pair agrees on: one queue, one policy and its key.
const LOOPBACK_QUEUE: &str = "orders";
const LOOPBACK_POLICY: &str = "RootManageSharedAccessKey";
const LOOPBACK_KEY: &str = "probe";

#[derive(Clone)]
pub struct ServiceBusTransport {
    endpoint: String,
    queue: String,
    policy: String,
    key: String,
    wait: u8,
    timeout: Option<Duration>,
}

impl ServiceBusTransport {
    /// Speak to the namespace at `endpoint` — `https://<ns>.servicebus.
    /// windows.net` in the cloud, `http://host:port` for a stand-in —
    /// about `queue`.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, queue: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            queue: queue.to_string(),
            policy: String::new(),
            key: String::new(),
            wait: 0,
            timeout: None,
        }
    }

    /// Sign as this shared access policy with its key.
    #[must_use]
    pub fn with_policy(mut self, policy: &str, key: &str) -> Self {
        self.policy = policy.to_string();
        self.key = key.to_string();
        self
    }

    /// Long-poll: wait up to `seconds` — [`MAX_WAIT`] at most — for a
    /// message where the queue is empty, rather than answering at once.
    #[must_use]
    pub const fn waiting(mut self, seconds: u8) -> Self {
        self.wait = if seconds > MAX_WAIT {
            MAX_WAIT
        } else {
            seconds
        };
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn client(&self) -> Result<Client> {
        let client = Client::new(&self.endpoint, &self.policy, &self.key)?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that holds this transport's policy and key, for a test or
    /// the playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.policy, &self.key);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// The queue a target names, or this transport's own where it names
    /// none.
    fn resolve<'a>(&'a self, target: &'a str) -> &'a str {
        if target.is_empty() {
            &self.queue
        } else {
            target
        }
    }
}

impl Transport for ServiceBusTransport {
    fn name(&self) -> &'static str {
        "azure-service-bus"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Up to [`MAX_MESSAGES`] messages, peek-locked one at a time and each
    /// completed once it is a Stream.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = self.client()?;
        let queue_url = client.resource(&self.queue);
        let mut arrived = Vec::new();
        while arrived.len() < MAX_MESSAGES {
            let Some(locked) = client.peek_lock(&self.queue, self.wait)? else {
                break;
            };
            client.complete(&self.queue, &locked.id, &locked.lock_token)?;
            arrived.push(Arrived::new(
                format!("{queue_url}#{}", locked.id),
                locked.body,
            ));
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > ceiling() {
            return Err(TransportError::permanent(format!(
                "{} bytes is over the {} one Service Bus message carries",
                bytes.len(),
                ceiling()
            )));
        }
        self.client()?.send(self.resolve(target), bytes)
    }
}

impl ServiceBusTransport {
    /// Both ends on this machine: an ephemeral local port, one policy and
    /// key the far end expects and the near end signs with, the loopback
    /// timeout.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("http://127.0.0.1:0", LOOPBACK_QUEUE)
            .with_policy(LOOPBACK_POLICY, LOOPBACK_KEY)
            .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

/// A bound session waiting for its one send.
struct Serving {
    session: Session,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Serving {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(mut self: Box<Self>) -> Result<Arrived> {
        match self.session.serve_one(&self.listener)? {
            Event::Sent(arrived) => Ok(arrived),
            Event::Refused(code) => Err(protocol_error(format!("the session refused: {code}"))),
            other => Err(protocol_error(format!("not a send: {other:?}"))),
        }
    }
}

impl Loopback for ServiceBusTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = socket::bind_tcp(&endpoint::authority(&self.endpoint)?)?;
        Ok(Box::new(Serving {
            session: self.session(),
            listener,
            address,
        }))
    }

    /// Send the payload as one message, from a fresh near end signing as
    /// this transport does, at the namespace on `address`.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self {
            endpoint: format!("http://{address}"),
            ..self.clone()
        };
        near.send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::JoinHandle;

    fn node(endpoint: &str, key: &str) -> ServiceBusTransport {
        ServiceBusTransport::new(endpoint, "orders")
            .with_policy("policy", key)
            .waiting(1)
            .timing_out_after(Duration::from_secs(2))
    }

    fn serve(
        mut session: Session,
        listener: TcpListener,
        requests: usize,
    ) -> JoinHandle<(Session, Vec<Event>)> {
        std::thread::spawn(move || {
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        })
    }

    #[test]
    fn what_is_sent_to_a_session_is_received_back_and_completed() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "secret");
        // Two sends; then a peek-lock and a complete per message, and the
        // peek-lock that finds the queue empty.
        let far_end = serve(near.session(), listener, 7);
        near.send("", b"UNA:+.? '").expect("its own queue");
        near.send("orders", &[0, 0xff, b'\r', b'\n'])
            .expect("a queue name");
        let arrived = near.receive().expect("received");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"UNA:+.? '");
        assert_eq!(arrived[1].bytes, [0, 0xff, b'\r', b'\n']);
        let queue_url = format!("http://{address}/orders");
        assert!(arrived[0].origin_uri.starts_with(&format!("{queue_url}#")));
        let (session, events) = far_end.join().expect("thread");
        assert!(session.messages().is_empty(), "completed after receive");
        assert_eq!(
            events[0],
            Event::Sent(Arrived::new(
                arrived[0].origin_uri.clone(),
                b"UNA:+.? '".to_vec()
            ))
        );
        assert!(matches!(
            &events[2],
            Event::Locked {
                id: Some(_),
                wait: 1,
                ..
            }
        ));
        assert_eq!(events[3], Event::Completed(arrived[0].origin_uri.clone()));
        assert!(matches!(&events[6], Event::Locked { id: None, .. }));
    }

    #[test]
    fn a_wrong_key_is_refused_with_service_buss_own_status_and_subcode() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = serve(node("http://x", "secret").session(), listener, 1);
        let failure = node(&format!("http://{address}"), "wrong")
            .send("", b"x")
            .expect_err("refused");
        assert!(failure.message.contains("401 40103"), "{failure}");
        assert!(!failure.retryable);
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(events, vec![Event::Refused("40103".to_string())]);
    }

    #[test]
    fn a_queue_is_not_claimed_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1", "secret");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "azure-service-bus");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(near.receive().expect_err("nothing listening").retryable);
        assert!(
            !node("ns.local", "s")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
        assert_eq!(node("http://x", "s").waiting(255).wait, MAX_WAIT);
    }

    #[test]
    fn what_is_over_the_ceiling_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1", "secret");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(failure.message.contains("262144"), "{failure}");
    }

    #[test]
    fn a_message_rounds_through_the_loopback_session() {
        let loopback = ServiceBusTransport::loopback();
        let arrived = loopback.round(b"UNA:+.? '").expect("round");
        assert_eq!(arrived.bytes, b"UNA:+.? '");
        assert!(
            arrived.origin_uri.starts_with("http://127.0.0.1:"),
            "{}",
            arrived.origin_uri
        );
        assert!(
            arrived.origin_uri.contains("/orders#"),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(loopback.name(), "azure-service-bus");
        assert_eq!(loopback.ceiling(), Some(ceiling()));
        assert!(loopback.refuses(&[0xff]).is_none());
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it, and one at the brim.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'x'; ceiling()]),
        ]
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_over_the_brim() {
        let loopback = ServiceBusTransport::loopback();
        for (name, payload) in edge_payloads() {
            assert!(loopback.refuses(&payload).is_none(), "{name}");
            let arrived = loopback.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        let over = vec![b'x'; ceiling() + 1];
        let failure = loopback.round(&over).expect_err("over the brim");
        assert!(failure.message.starts_with("send failed:"), "{failure}");
        assert!(failure.message.contains("262144"), "{failure}");
    }
}
