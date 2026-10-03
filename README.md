# xmip-core-transport-azure-service-bus

Azure Service Bus transport: a Shared Access Signature over the REST API — send a Stream as a message, receive with peek-lock and complete each message once the runtime accepts it — a queue is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The Shared Access Signature and the judgement of a namespace's answers come from [xmip-core-transport-azure](https://github.com/IlleNilsson/xmip-core-transport-azure), where every Azure technology shares what Azure speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

The namespace an origin names is `azure::sas::Token::namespace`; until 2026-09-28 this technology and Event Hubs each cut a token's resource by hand.

## How a received message is acknowledged

A receive peek-locks each message and completes none; it never receives and deletes. Every message it hands on is whole and stays locked until the runtime gives its verdict after the whole receive cycle (runtime-model section 5). Accepted completes it (`DELETE …/messages/<id>/<lock>`); Refused completes it too: dead-lettering is a settlement of the AMQP protocol and the SDKs, and the REST API this crate speaks has no dead-letter call, so a refused message is completed and not received again; the runtime has audited the refusal, and from Message creation on the Stream is kept in Xmip (ADR-0013). Failed unlocks it (`PUT …/messages/<id>/<lock>`), so the next peek-lock is handed it again rather than after the lock lapses. A crash before the verdict leaves the message to its lock's lapse: at-least-once, never a loss. The complete is the one the receive made until 2026-10-02; the unlock is one more request on a kept connection, made only for a failed cycle.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
