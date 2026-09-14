//! The far end: enough of Service Bus to answer one Location, and what a
//! test or the playground puts on loopback.
//!
//! Not Service Bus. One session holds the messages of every queue it is
//! asked about in memory, verifies every request's token against one
//! policy, and answers the three calls with the shapes the service answers
//! them — 201 for a send, the locked message with its `BrokerProperties`
//! for a peek-lock and 204 where the queue is empty, 200 for a complete,
//! the XML error with its subcode. A peek-lock that finds nothing answers
//! at once and records the wait it was asked for rather than holding the
//! connection; a locked message stays on the queue until it is completed,
//! as the service keeps it.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use serde_json::json;
use transport::Arrived;
use transport::error::Result;

use crate::ceiling;
use crate::client::chosen_id;
use crate::properties::{self, BROKER_PROPERTIES};
use http::message::{Request, Response};
use http::namespace::{self, subcode};
use http::sas::{self, Signer, Token};
use http::server;

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client sent a message; here is the Stream, its origin the queue
    /// URL and the id it was given.
    Sent(Arrived),
    /// The client peek-locked the head of `queue`, asking to wait `wait`
    /// seconds where there was none; `id` is the message it was handed.
    Locked {
        queue: String,
        id: Option<String>,
        wait: u8,
    },
    /// The client completed this message.
    Completed(String),
    /// The client was answered with this error subcode.
    Refused(String),
}

/// One message held, on its queue.
#[derive(Clone, Debug)]
struct Held {
    id: String,
    sequence: u64,
    body: Vec<u8>,
    lock: Option<String>,
}

pub struct Session {
    signer: Signer,
    queues: BTreeMap<String, Vec<Held>>,
    next: u64,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests whose token `policy` made with `key`.
    #[must_use]
    pub fn new(policy: &str, key: &str) -> Self {
        Self {
            signer: Signer::new(policy, key),
            queues: BTreeMap::new(),
            next: 1,
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Every message held now, keyed `queue#id`, locked or not.
    #[must_use]
    pub fn messages(&self) -> BTreeMap<String, Vec<u8>> {
        self.queues
            .iter()
            .flat_map(|(queue, held)| {
                held.iter()
                    .map(move |m| (format!("{queue}#{}", m.id), m.body.clone()))
            })
            .collect()
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        let token = match self.signer.verify(request, sas::now()) {
            Ok(token) => token,
            Err(failure) => return refused(401, &failure.message),
        };
        let path = request.path.strip_prefix('/').unwrap_or(&request.path);
        let Some((queue, rest)) = path.rsplit_once("/messages") else {
            return refused(404, "40400: Not a queue's messages");
        };
        let origin = origin(&token, queue);
        match (request.method.as_str(), rest) {
            ("POST", "") => self.send(queue, &origin, request),
            ("POST", "/head") => self.peek_lock(queue, &origin, request),
            ("DELETE", "/head") => self.receive_and_delete(queue, &origin),
            ("DELETE", locked) => self.complete(queue, &origin, locked),
            _ => refused(405, "40500: Not one of the three calls"),
        }
    }

    fn send(&mut self, queue: &str, origin: &str, request: &Request) -> (Event, Response) {
        if request.body.len() > ceiling() {
            return refused(
                413,
                &format!("40000: A message is at most {} bytes", ceiling()),
            );
        }
        let properties = match properties::properties_of(request) {
            Ok(properties) => properties,
            Err(failure) => return refused(400, &format!("40000: {}", failure.message)),
        };
        let sequence = self.next;
        self.next += 1;
        let id = chosen_id(&properties).unwrap_or_else(|| format!("{sequence:08x}-xmip"));
        let held = Held {
            id: id.clone(),
            sequence,
            body: request.body.clone(),
            lock: None,
        };
        self.queues.entry(queue.to_string()).or_default().push(held);
        (
            Event::Sent(Arrived::new(format!("{origin}#{id}"), request.body.clone())),
            Response::new(201),
        )
    }

    fn peek_lock(&mut self, queue: &str, origin: &str, request: &Request) -> (Event, Response) {
        let wait = request
            .query_value("timeout")
            .and_then(|w| w.parse().ok())
            .unwrap_or(0);
        let next = self.next;
        let held = self.queues.entry(queue.to_string()).or_default();
        let Some(message) = held.iter_mut().find(|m| m.lock.is_none()) else {
            return (locked(queue, None, wait), Response::new(204));
        };
        let lock = format!("lock-{next:08x}");
        self.next += 1;
        message.lock = Some(lock.clone());
        let properties = json!({
            "MessageId": message.id,
            "LockToken": lock,
            "SequenceNumber": message.sequence,
            "DeliveryCount": 1,
        });
        let response = Response::new(201)
            .header(BROKER_PROPERTIES, &properties.to_string())
            .header("Content-Type", "application/octet-stream")
            .header(
                "Location",
                &format!("{origin}/messages/{}/{lock}", message.id),
            )
            .body(&message.body);
        (locked(queue, Some(message.id.clone()), wait), response)
    }

    fn receive_and_delete(&mut self, queue: &str, origin: &str) -> (Event, Response) {
        let held = self.queues.entry(queue.to_string()).or_default();
        let Some(at) = held.iter().position(|m| m.lock.is_none()) else {
            return (locked(queue, None, 0), Response::new(204));
        };
        let message = held.remove(at);
        let properties = json!({ "MessageId": message.id, "SequenceNumber": message.sequence });
        let response = Response::new(200)
            .header(BROKER_PROPERTIES, &properties.to_string())
            .body(&message.body);
        (
            Event::Completed(format!("{origin}#{}", message.id)),
            response,
        )
    }

    fn complete(&mut self, queue: &str, origin: &str, locked: &str) -> (Event, Response) {
        let held = self.queues.entry(queue.to_string()).or_default();
        let at = locked
            .strip_prefix('/')
            .and_then(|rest| rest.split_once('/'))
            .and_then(|(id, lock)| {
                held.iter()
                    .position(|m| m.id == id && m.lock.as_deref() == Some(lock))
            });
        match at {
            Some(at) => {
                let message = held.remove(at);
                (
                    Event::Completed(format!("{origin}#{}", message.id)),
                    Response::new(200),
                )
            }
            None => refused(404, "40400: No such message under that lock"),
        }
    }
}

/// The queue `queue` as an origin names it: the token's scheme and
/// authority, then the queue's path.
fn origin(token: &Token, queue: &str) -> String {
    let base = token
        .resource
        .split_once("://")
        .map_or(token.resource.as_str(), |(scheme, rest)| {
            &token.resource[..scheme.len() + 3 + rest.find('/').unwrap_or(rest.len())]
        });
    format!("{base}/{queue}")
}

fn locked(queue: &str, id: Option<String>, wait: u8) -> Event {
    Event::Locked {
        queue: queue.to_string(),
        id,
        wait,
    }
}

fn refused(status: u16, detail: &str) -> (Event, Response) {
    (
        Event::Refused(subcode(detail)),
        namespace::error(status, detail),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESOURCE: &str = "http://ns.local/orders";

    fn signed(method: &str, path: &str) -> Request {
        Signer::new("policy", "secret").sign(
            Request::new(method, path).header("Host", "ns.local"),
            RESOURCE,
            sas::now() + 60,
        )
    }

    #[test]
    fn a_session_answers_in_service_buss_shapes_and_refuses_a_bad_token() {
        let mut session = Session::new("policy", "secret");
        let sent = signed("POST", "/orders/messages").body(b"a<b");
        let (event, response) = session.answer(&sent);
        assert_eq!(response.status, 201);
        assert_eq!(
            event,
            Event::Sent(Arrived::new(
                "http://ns.local/orders#00000001-xmip",
                b"a<b".to_vec()
            ))
        );
        let chosen = signed("POST", "/orders/messages")
            .header(BROKER_PROPERTIES, r#"{"MessageId":"mine"}"#)
            .body(b"");
        let (event, _) = session.answer(&chosen);
        assert_eq!(
            event,
            Event::Sent(Arrived::new("http://ns.local/orders#mine", Vec::new()))
        );
        let peek = signed("POST", "/orders/messages/head").query("timeout", "5");
        let (event, response) = session.answer(&peek);
        assert_eq!(response.status, 201);
        assert_eq!(response.body, b"a<b");
        let properties = properties::properties_in(&response).expect("properties");
        assert_eq!(properties["MessageId"], "00000001-xmip");
        assert_eq!(properties["SequenceNumber"], 1);
        let lock = properties["LockToken"]
            .as_str()
            .expect("a lock")
            .to_string();
        assert!(
            response
                .header_value("location")
                .expect("a location")
                .ends_with(&format!("/orders/messages/00000001-xmip/{lock}"))
        );
        assert!(matches!(
            event,
            Event::Locked {
                wait: 5,
                id: Some(_),
                ..
            }
        ));
        let (event, response) = session.answer(&signed("DELETE", "/orders/messages/head"));
        assert_eq!(
            response.status, 200,
            "receive and delete takes the unlocked one"
        );
        assert_eq!(
            event,
            Event::Completed("http://ns.local/orders#mine".to_string())
        );
        let (event, response) = session.answer(&peek);
        assert_eq!(response.status, 204, "locked, not offered again");
        assert!(matches!(event, Event::Locked { id: None, .. }));
        let complete = signed("DELETE", &format!("/orders/messages/00000001-xmip/{lock}"));
        let (event, response) = session.answer(&complete);
        assert_eq!(response.status, 200);
        assert_eq!(
            event,
            Event::Completed("http://ns.local/orders#00000001-xmip".to_string())
        );
        assert!(session.messages().is_empty());
        let (event, response) = session.answer(&complete);
        assert_eq!(
            (event, response.status),
            (Event::Refused("40400".to_string()), 404)
        );
        let (_, response) = session.answer(&signed("PUT", "/orders/messages/head"));
        assert_eq!(response.status, 405);
        let (_, response) = session.answer(&signed("GET", "/orders"));
        assert_eq!(response.status, 404);
        let over = signed("POST", "/orders/messages").body(&vec![0; ceiling() + 1]);
        let (event, response) = session.answer(&over);
        assert_eq!(
            (event, response.status),
            (Event::Refused("40000".to_string()), 413)
        );
        let wrong = Signer::new("policy", "wrong").sign(
            Request::new("POST", "/orders/messages"),
            RESOURCE,
            sas::now() + 60,
        );
        let (event, response) = session.answer(&wrong);
        assert_eq!(
            (event, response.status),
            (Event::Refused("40103".to_string()), 401)
        );
    }
}
