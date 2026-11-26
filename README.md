# xmip-core-transport-azure-service-bus

Azure Service Bus transport: a Shared Access Signature over the REST API — send a Stream as a message, receive with peek-lock and complete each message once the runtime accepts it — a queue is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location sends to the queue its `queue` setting names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which it reads, through `transport::destination::named`, as that queue, its address naming only the namespace. A target naming another queue still wins (ADR-0064, amendment 2026-11-10).

A Location signs under the shared access policy its `key_name` setting names, `RootManageSharedAccessKey` or one of its own, with the policy's key its `secret = "<name>"` names: a `password` secret kept in the Administration Database and never in configuration, read at each request, a send and a receive's peek-lock, complete and unlock alike, so a key set again reaches the next request without a restart (ADR-0019, amendment 2026-10-29; ADR-0015, amendment 2026-11-25). A Location naming a secret and no `key_name` is refused as the node starts: a Shared Access Signature names its policy.

The Shared Access Signature and the judgment of a namespace's answers come from [xmip-core-transport-azure](https://github.com/IlleNilsson/xmip-core-transport-azure), where every Azure technology shares what Azure speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

The namespace an origin names is `azure::sas::Token::namespace`; until 2026-09-28 this technology and Event Hubs each cut a token's resource by hand.

## How a received message is acknowledged

A receive peek-locks each message and completes none; it never receives and deletes. Every message it hands on is whole and stays locked until the runtime gives its verdict after the whole receive cycle (runtime-model section 5). Accepted completes it (`DELETE …/messages/<id>/<lock>`); Refused completes it too: dead-lettering is a settlement of the AMQP protocol and the SDKs, and the REST API this crate speaks has no dead-letter call, so a refused message is completed and not received again; the runtime has audited the refusal, and from Message creation on the Stream is kept in Xmip (ADR-0013). Failed unlocks it (`PUT …/messages/<id>/<lock>`), so the next peek-lock is handed it again rather than after the lock lapses. A crash before the verdict leaves the message to its lock's lapse: at-least-once, never a loss. The complete is the one the receive made until 2026-10-02; the unlock is one more request on a kept connection, made only for a failed cycle.

## The deduplication key

A keyed send (the key `Transport::send_from` is handed, `Outgoing::key`; built 2026-10-04) carries the Journey's identifier as the message's `MessageId`, in its `BrokerProperties` header, the same on every attempt on one Journey. A queue with duplicate detection on drops a message whose `MessageId` it has seen within its detection window; one without keeps both. An unkeyed `send` sends no `BrokerProperties`, and the namespace gives the message an id of its own. The in-process `Session` keeps the id a client chose, as it did.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `key_name`, `queue`, `wait_seconds` and `timeout`, and `SCHEMES`, `azure-service-bus`; `key_name` is the Azure crate's, which this crate's declaration takes by that crate's own `declaration` feature, and `wait_seconds` is held to the crate's `MAX_WAIT`, built with the declaration. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the http technology, and with it TLS — are behind the default feature, and `tls`, which is code, turns the default on with it. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check a Service Bus Location's settings, its `key_name` among them, though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
