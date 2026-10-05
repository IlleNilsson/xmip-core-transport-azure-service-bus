//! A keyed send carries its deduplication key as the message's
//! `MessageId`, the same on every attempt of one Journey; an unkeyed send
//! carries none, and the namespace gives it one.

use std::thread;

use transport::Transport;
use transport::socket;
use xmip_core_transport_azure_service_bus::{Event, ServiceBusTransport};

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_message_carries_the_journey_id_as_its_message_id_on_every_attempt() {
    let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bound");
    let near = ServiceBusTransport::new(format!("http://{address}"), "orders")
        .with_policy("policy", "secret");
    let mut session = near.session();
    let far_end = thread::spawn(move || {
        (0..3)
            .map(|_| match session.serve_one(&listener).expect("served") {
                Event::Sent(taken) => taken.origin_uri,
                other => panic!("not a send: {other:?}"),
            })
            .collect::<Vec<_>>()
    });
    near.send_keyed("", b"order", KEY).expect("sent");
    near.send_keyed("", b"order", KEY).expect("sent again");
    near.send("", b"order").expect("sent unkeyed");
    let ids: Vec<String> = far_end
        .join()
        .expect("far end")
        .iter()
        .map(|origin| origin.split_once('#').expect("an id").1.to_string())
        .collect();
    assert_eq!(ids[..2], [KEY, KEY]);
    assert_ne!(ids[2], KEY, "an unkeyed message is given an id of its own");
}
