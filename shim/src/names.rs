//! Public domain names over fips (`www.example.org` → a mesh node), the
//! phone half of fr34aky/fips-names: `names-core` decides, `names-resolve`
//! talks to relays and the legacy DNS, and this module supplies what only
//! the phone can — the mesh transport (`meshudp.rs`) and the identity
//! registration through the in-process responder — and runs the lookup
//! from the DNS proxy's blocking per-query thread.
//!
//! Order of business for a non-`.fips` name (fips-names spec §5–§7): local
//! pins, then — online — the `_fips-dns.<domain>` TXT record from the
//! configured upstreams, a claim from the relays only after a TXT hit, step
//! 3 to the domain's server over the mesh, and a synthesized answer with the
//! node's `fd…` address. Everything else returns `None`, and the proxy
//! forwards to the upstreams exactly as before: a name that is not over
//! fips is never made unreachable by this code.

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

/// The whole lookup must fit inside what an app's resolver waits for (5 s on
/// bionic) with room for the legacy fallback after it.
const BUDGET: Duration = Duration::from_millis(3500);

/// What the DNS proxy asks: a complete reply, or "not ours".
pub trait Lookup: Send + Sync {
    fn lookup(&self, query: &[u8]) -> Option<Vec<u8>>;
}

pub struct Names {
    /// One worker thread: hickory and nostr-sdk need an async runtime, the
    /// proxy is blocking, and `block_on` from its thread drives the future
    /// while the worker keeps the relay pool alive.
    rt: Option<tokio::runtime::Runtime>,
    resolver: Arc<Resolver<MaybeTxt, RelayClient>>,
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
        let upstreams: Vec<IpAddr> = config
            .upstream_addrs()
            .into_iter()
            .map(|a| a.ip())
            .collect();
        let rc = ResolverConfig {
            public_relays: if config.nostr_relays.is_empty() {
                ResolverConfig::default().public_relays
            } else {
                config.nostr_relays.clone()
            },
            mesh_relays: config.names_mesh_relays.clone(),
            allow_unverified_offline: config.names_allow_unverified_offline,
            ..ResolverConfig::default()
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
        Ok(Arc::new(Self { rt: Some(rt), resolver: Arc::new(resolver) }))
    }

    /// The network moved: what was "unreachable" may answer now, and vice
    /// versa. Pins are untouched.
    pub fn network_changed(&self) {
        self.resolver.flush_caches();
    }
}

impl Lookup for Names {
    fn lookup(&self, query: &[u8]) -> Option<Vec<u8>> {
        let rt = self.rt.as_ref()?;
        let resolver = self.resolver.clone();
        let q = query.to_vec();
        match rt.block_on(async move {
            tokio::time::timeout(BUDGET, resolver.lookup(&q)).await
        }) {
            Ok(LookupResult::Answer(a)) => Some(a),
            Ok(LookupResult::Passthrough) => None,
            Err(_) => {
                tracing::warn!("public-name lookup exceeded its budget; using the legacy answer");
                None
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
