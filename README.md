# xmip-core-transport-azure-service-bus

Azure Service Bus transport: a Shared Access Signature over the REST API — send a Stream as a message, receive with peek-lock and complete each message once it is a Stream — a queue is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The Shared Access Signature and the judgement of a namespace's answers come from [xmip-core-transport-azure](https://github.com/IlleNilsson/xmip-core-transport-azure), where every Azure technology shares what Azure speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
