//! Public domain names over fips (`www.example.org` → a mesh node), the
//! phone half of fr34aky/fips-pub-domains: `pubdom-core` decides,
//! `pubdom-resolve` talks to relays and the legacy DNS, and this module
//! supplies what only
//! the phone can — the mesh transport (`meshudp.rs`) and the identity
//! registration through the in-process responder — and runs the lookup
//! from the DNS proxy's blocking per-query thread.
//!
//! Order of business for a non-`.fips` name (fips-pub-domains spec §5–§7):
//! local pins, then — online — the `_fips-dns.<domain>` TXT record from the
//! configured upstreams, a claim from the relays only after a TXT hit (or,
//! offline, a claim from a relay on the mesh whose DNSSEC proof verifies),
//! step
//! 3 to the domain's server over the mesh, and a synthesized answer with the
//! node's `fd…` address. Everything else is [`Outcome::Legacy`], and the
//! proxy forwards to the upstreams exactly as before — or
//! [`Outcome::Pending`] when the lookup overran its budget and is still
//! deciding, which forwards too but with the answer's TTLs capped. A name
//! that is not over fips is never made unreachable by this code.

use std::net::{IpAddr, SocketAddrV6, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use pubdom_core::Npub;
use pubdom_resolve::config::MaybeTxt;
use pubdom_resolve::mesh::MeshDns;
use pubdom_resolve::relay::RelayClient;
use pubdom_resolve::resolver::{LookupResult, Resolver, ResolverConfig};
use pubdom_resolve::FilePinStore;

use crate::config::ShimConfig;
use crate::meshhttp::MeshLink;
use crate::meshtcp::MeshRelayProxy;

/// The whole lookup must fit inside what an app's resolver waits for (5 s on
/// bionic) with room for the legacy fallback after it.
const BUDGET: Duration = Duration::from_millis(3500);

/// What the DNS proxy asks: a complete reply, or what to do with the
/// legacy one.
pub enum Outcome {
    /// A complete reply for the application.
    Answer(Vec<u8>),
    /// Not over fips: the upstreams' answer, unchanged.
    Legacy,
    /// Still deciding (the lookup overran its budget and carries on): the
    /// upstreams' answer, but with its TTLs capped at
    /// `pubdom_core::OVERRUN_TTL_SECS` so the application's resolver asks
    /// again about when the decision is in. Android kept a parked
    /// wildcard's address for its 300 s otherwise, and the cached decision
    /// was never asked for.
    Pending,
}

pub trait Lookup: Send + Sync {
    fn lookup(&self, query: &[u8]) -> Outcome;
}

pub struct Names {
    /// One worker thread: hickory and nostr-sdk need an async runtime, the
    /// proxy is blocking, and `block_on` from its thread drives the future
    /// while the worker keeps the relay pool alive.
    rt: Option<tokio::runtime::Runtime>,
    resolver: Arc<Resolver<MaybeTxt, RelayClient>>,
    /// The mesh relays' loopback listeners; they stop when this drops.
    _relays: Vec<MeshRelayProxy>,
}

impl Names {
    /// Build the resolver over `link`; `pins_path` is the app-private JSON
    /// pin file (shared schema with the desktop daemon).
    pub fn start(config: &ShimConfig, link: Arc<MeshLink>, pins_path: &str) -> Result<Arc<Self>, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("fips-names")
            .enable_all()
            .build()
            .map_err(|e| format!("names runtime: {e}"))?;
        // Relays on the mesh (`ws://<npub>.fips:port`): nostr-sdk's sockets
        // cannot reach fd00::/8 from this app, so each gets a loopback
        // listener carried over the in-process TCP stack (`meshtcp.rs`).
        let proxies: Vec<MeshRelayProxy> = config
            .names_mesh_relays
            .iter()
            .filter_map(|url| match MeshRelayProxy::start(link.clone(), url) {
                Ok(p) => Some(p),
                Err(e) => {
                    tracing::warn!(error = %e, "mesh relay not used");
                    None
                }
            })
            .collect();
        let upstreams: Vec<IpAddr> = config
            .upstream_addrs()
            .into_iter()
            .map(|a| a.ip())
            .collect();
        // A witness that does not parse is dropped with a warning rather
        // than failing the start: the app validates on the way out, and a
        // name-resolution feature must not keep the tunnel from coming up.
        // The line itself is not logged: the log ring is shareable from
        // Diagnostics, and a pasted secret must not end up in it.
        let witnesses: Vec<Npub> = config
            .names_witnesses
            .iter()
            .enumerate()
            .filter_map(|(i, w)| match Npub::parse_any(w.trim()) {
                Ok(n) => Some(n),
                Err(e) => {
                    tracing::warn!(line = i + 1, len = w.len(), error = %e, "ignoring witness: not an npub");
                    None
                }
            })
            .collect();
        let defaults = ResolverConfig::default();
        let rc = ResolverConfig {
            public_relays: if config.nostr_relays.is_empty() {
                defaults.public_relays.clone()
            } else {
                config.nostr_relays.clone()
            },
            mesh_relays: proxies.iter().map(|p| p.local_url.clone()).collect(),
            allow_unverified_offline: config.names_allow_unverified_offline,
            witnesses,
            attestation_threshold: config
                .names_attestation_threshold
                .unwrap_or(defaults.attestation_threshold),
            dnssec: config.names_dnssec.unwrap_or(defaults.dnssec),
            ..defaults
        };
        let pins = FilePinStore::open(pins_path).map_err(|e| format!("pins {pins_path}: {e}"))?;
        // The upstreams are fixed for the engine's lifetime: a network change
        // rebinds the whole engine with a fresh config, so nothing swaps them
        // at run time here.
        let txt = MaybeTxt::new(&upstreams, rc.dnssec, rc.txt_timeout)?;
        let resolver = rt.block_on(async {
            let relays = RelayClient::new(&rc.public_relays, &rc.mesh_relays, rc.relay_timeout).await;
            Resolver::new(rc, Arc::new(pins), txt, relays, Arc::new(PhoneMesh { link }))
        });
        Ok(Arc::new(Self { rt: Some(rt), resolver: Arc::new(resolver), _relays: proxies }))
    }

    /// The network moved: what was "unreachable" may answer now, and vice
    /// versa. Pins are untouched.
    pub fn network_changed(&self) {
        self.resolver.flush_caches();
    }
}

impl Lookup for Names {
    fn lookup(&self, query: &[u8]) -> Outcome {
        let Some(rt) = self.rt.as_ref() else {
            return Outcome::Legacy;
        };
        let resolver = self.resolver.clone();
        let q = query.to_vec();
        // Spawned, not awaited in place: when the budget runs out the app
        // gets the legacy answer (short-lived, see `Outcome::Pending`), but
        // the lookup carries on and caches its decision — so the query the
        // resolver makes after that is answered at once. Cancelling it
        // instead meant a slow path (offline: TXT timeout, then a mesh
        // relay) never finished, however often it was asked.
        let task = rt.spawn(async move { resolver.lookup(&q).await });
        match rt.block_on(async { tokio::time::timeout(BUDGET, task).await }) {
            Ok(Ok(LookupResult::Answer(a))) => Outcome::Answer(a),
            Ok(Ok(LookupResult::Passthrough)) => Outcome::Legacy,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "public-name lookup failed; using the legacy answer");
                Outcome::Legacy
            }
            Err(_) => {
                tracing::info!("public-name lookup still running after its budget; using the legacy answer briefly");
                Outcome::Pending
            }
        }
    }
}

impl Drop for Names {
    fn drop(&mut self) {
        // Never wait for stragglers in the blocking pool (see the node
        // thread's shutdown_timeout in engine.rs for why).
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

/// The mesh transport: `meshudp` for step 3, the responder for registration.
struct PhoneMesh {
    link: Arc<MeshLink>,
}

impl MeshDns for PhoneMesh {
    fn query_udp(&self, server: SocketAddrV6, msg: &[u8], timeout: Duration) -> std::io::Result<Vec<u8>> {
        crate::meshudp::query(&self.link, *server.ip(), server.port(), msg, timeout)
            .map_err(|e| std::io::Error::other(e.message))
    }

    fn query_tcp(&self, _: SocketAddrV6, _: &[u8], _: Duration) -> std::io::Result<Vec<u8>> {
        // A step 3 answer is one CNAME; nothing truncates it. The TCP
        // fallback rides on meshhttp's stack in a later milestone.
        Err(std::io::Error::other("TCP fallback not available on the phone"))
    }

    fn reachable(&self, npub: Npub, timeout: Duration) -> bool {
        crate::meshudp::ping(&self.link, npub.fips_address(), timeout).unwrap_or(false)
    }

    fn register(&self, npub: Npub, timeout: Duration) -> bool {
        // Same exchange as meshhttp::register_identity, but the outcome
        // matters here: it doubles as the reachability signal (spec §7).
        let Ok(socket) = UdpSocket::bind(if self.link.responder.is_ipv6() { "[::1]:0" } else { "127.0.0.1:0" }) else {
            return false;
        };
        let _ = socket.set_read_timeout(Some(timeout));
        let id = (crate::meshhttp::random_u64() & 0xffff) as u16;
        let query = crate::meshhttp::aaaa_query(id, &npub.fips_name());
        if socket.send_to(&query, self.link.responder).is_err() {
            return false;
        }
        let mut buf = [0u8; 512];
        match socket.recv(&mut buf) {
            Ok(n) if n >= 12 && buf[..2] == id.to_be_bytes() => buf[3] & 0x0f == 0,
            _ => false,
        }
    }
}
