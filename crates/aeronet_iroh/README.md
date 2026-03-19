# `aeronet_iroh`

Native [`iroh`](https://crates.io/crates/iroh) IO layer integration for `aeronet`.

This crate plugs iroh's QUIC datagram connections into `aeronet_io` sessions so
that higher layers like `aeronet_transport` can run unchanged.

## Scope

- native-only
- unreliable datagram transport
- shared iroh endpoints supported
- client and server Bevy plugins

## Status

This crate currently focuses on the core session plumbing and a minimal Bevy API
mirroring the structure of the existing transport crates in this workspace.
