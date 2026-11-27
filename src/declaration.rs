//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`. `key_name`
//! is the Azure crate's, taken by that crate's own `declaration` feature;
//! `wait_seconds` is held to [`MAX_WAIT`].

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

use crate::MAX_WAIT;

/// Every setting a Location takes beyond its address, the namespace,
/// `https://<ns>.servicebus.windows.net`. The shared access policy is the
/// `key_name` setting, and its key the Location's `secret`, a password
/// secret the Administration Database keeps, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        azure::key_name::key_name(Applies::Both),
        Setting {
            name: "queue",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The queue received from, and sent to when a send target names none.",
            applies: Applies::Both,
        },
        Setting {
            name: "wait_seconds",
            kind: Kind::Integer {
                minimum: 0,
                maximum: MAX_WAIT as i64,
            },
            presence: Presence::Optional,
            meaning: "How many seconds a receive long-polls an empty queue for a \
                      message; it answers at once when left out.",
            applies: Applies::Receive,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["azure-service-bus"];
