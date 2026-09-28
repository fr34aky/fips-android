//! A long-lived TCP stream to a mesh node, piped to a local socket — how
//! the public-names resolver reaches Nostr relays that run on fips nodes.
//!
//! nostr-sdk opens ordinary sockets, and the app's own sockets cannot reach
//! `fd00::/8` (see `meshhttp.rs` for why). So each mesh relay gets a
//! listener on loopback; every connection accepted there is carried over a
//! userspace TCP stack (smoltcp, the same device and packet path as
//! `meshhttp.rs`) to the relay's fips address. The bytes are opaque here —
//! the websocket handshake and everything after it pass through unchanged.
//!
//! Idle cost matters on a phone: a relay connection stays open, so the pipe
//! blocks on one event channel (mesh packets and local reads alike) until
//! something arrives or smoltcp has a timer due, instead of polling.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{Ipv6Addr, Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fips::TunPacketAction;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};

use crate::meshhttp::{is_icmp_unreachable, random_u64, register_identity, Device, FetchError, MeshLink};

/// How long a connection may take to be accepted by the far node.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest wait with nothing to do: bounds how late a stopped engine or a
/// closed listener is noticed on an idle connection.
const IDLE_WAIT: Duration = Duration::from_secs(5);
/// Keep-alive on the mesh side, so a relay connection that went quiet is
/// found dead instead of hanging on.
const KEEPALIVE: Duration = Duration::from_secs(60);
/// Bytes from the mesh not yet written to the local side before the pipe
/// stops reading from the mesh (TCP backpressure does the rest).
const LOCAL_BACKLOG: usize = 256 * 1024;

/// A relay on the mesh, served on a loopback port for the lifetime of the
/// value; dropping it stops the listener (open connections end with it).
pub struct MeshRelayProxy {
    pub local_url: String,
    stop: Arc<AtomicBool>,
}

impl Drop for MeshRelayProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// A mesh relay URL: `ws://<npub>.fips[:port][/path]`. Other forms (a
/// public `wss://` relay, an `[fd…]` literal) are not mesh relays here.
pub fn parse_mesh_relay(url: &str) -> Option<(String, u16, String)> {
    let rest = url.strip_prefix("ws://")?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, String::new()),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().ok().filter(|p| *p != 0)?),
        None => (hostport, 80),
    };
    let npub = host.to_ascii_lowercase().strip_suffix(".fips")?.to_string();
    pubdom_core::Npub::parse(&npub).ok()?;
    Some((npub, port, path))
}

impl MeshRelayProxy {
    /// Listen on loopback for `url` (a `ws://<npub>.fips…` relay).
    pub fn start(link: Arc<MeshLink>, url: &str) -> Result<Self, String> {
        let (npub, port, path) = parse_mesh_relay(url).ok_or_else(|| format!("{url}: not a ws://<npub>.fips relay"))?;
        let addr = pubdom_core::Npub::parse(&npub)
            .map_err(|e| format!("{url}: {e}"))?
            .fips_address();
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| format!("{url}: listen: {e}"))?;
        let local = listener.local_addr().map_err(|e| e.to_string())?;
        // Nonblocking accept, so the thread notices `stop` without a
        // connection arriving.
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let stop = Arc::new(AtomicBool::new(false));
        let st = stop.clone();
        let name = url.to_string();
        std::thread::Builder::new()
            .name("mesh-relay".into())
            .spawn(move || accept_loop(listener, link, npub, addr, port, st, name))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            local_url: format!("ws://{local}{path}"),
            stop,
        })
    }
}

fn accept_loop(
    listener: TcpListener,
    link: Arc<MeshLink>,
    npub: String,
    addr: Ipv6Addr,
    port: u16,
    stop: Arc<AtomicBool>,
    name: String,
) {
    while !stop.load(Ordering::Relaxed) && link.running.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let (link, npub, stop, name) = (link.clone(), npub.clone(), stop.clone(), name.clone());
                let _ = std::thread::Builder::new().name("mesh-relay-conn".into()).spawn(move || {
                    if let Err(e) = pipe(&link, &npub, addr, port, stream, &stop) {
                        tracing::info!(relay = %name, error = %e.message, "mesh relay connection ended");
                    }
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(250));
            }
            Err(e) => {
                tracing::warn!(relay = %name, error = %e, "mesh relay listener failed");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

enum Event {
    Mesh(Vec<u8>),
    Local(Vec<u8>),
    LocalClosed,
}

/// Carry `local` to `[addr]:port` over the mesh until either side closes.
fn pipe(
    link: &MeshLink,
    npub: &str,
    addr: Ipv6Addr,
    port: u16,
    local: TcpStream,
    stop: &AtomicBool,
) -> Result<(), FetchError> {
    if addr == link.our_addr {
        return Err(FetchError::other("that is this node's own address"));
    }
    let _ = local.set_nodelay(true);
    register_identity(link.responder, npub);
    let mut flow = link
        .divert
        .open(addr, port)
        .ok_or_else(|| FetchError::other("no free local port"))?;
    let local_port = flow.key.2;

    // One channel for everything the pipe waits on.
    let (tx, events) = mpsc::channel::<Event>();
    {
        // Mesh packets for this flow. Ends when the flow guard drops.
        let (tx, rx) = (tx.clone(), flow.take_rx());
        std::thread::spawn(move || {
            while let Ok(p) = rx.recv() {
                if tx.send(Event::Mesh(p)).is_err() {
                    break;
                }
            }
        });
    }
    {
        // Local reads. Ends at EOF, on an error, or when the pipe shuts the
        // socket down.
        let mut reader = local.try_clone().map_err(|e| FetchError::other(e.to_string()))?;
        let tx = tx.clone();
        std::thread::spawn(move || {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => {
                        let _ = tx.send(Event::LocalClosed);
                        break;
                    }
                    Ok(n) => {
                        if tx.send(Event::Local(buf[..n].to_vec())).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }
    drop(tx);

    let processor = link.processor.clone();
    let outbound = link.outbound_tx.clone();
    let egress = move |mut packet: Vec<u8>| -> Result<(), FetchError> {
        match processor.process(&mut packet) {
            TunPacketAction::Forward => outbound.blocking_send(packet).map_err(|_| FetchError::restarted()),
            TunPacketAction::Hairpin => Err(FetchError::other("that is this node's own address")),
            TunPacketAction::Respond(_) => Err(FetchError::unreachable(format!("the node refused to send to {addr}"))),
            TunPacketAction::Drop => Ok(()),
        }
    };
    let result = run(egress, &events, (link.our_addr, local_port), (addr, port), &local, &link.running, stop);
    let _ = local.shutdown(Shutdown::Both);
    drop(flow);
    result
}

fn run<E: FnMut(Vec<u8>) -> Result<(), FetchError>>(
    egress: E,
    events: &mpsc::Receiver<Event>,
    local_addr: (Ipv6Addr, u16),
    remote: (Ipv6Addr, u16),
    mut local: &TcpStream,
    alive: &AtomicBool,
    stop: &AtomicBool,
) -> Result<(), FetchError> {
    let start = Instant::now();
    let now = || smoltcp::time::Instant::from_millis(start.elapsed().as_millis() as i64);
    let mut device = Device {
        rx: VecDeque::new(),
        egress,
        failed: None,
    };
    let mut config = Config::new(HardwareAddress::Ip);
    config.random_seed = random_u64();
    let mut iface = Interface::new(config, &mut device, now());
    iface.update_ip_addrs(|addrs| {
        let _ = addrs.push(IpCidr::new(IpAddress::Ipv6(local_addr.0), 8));
    });
    let mut socket = tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; 64 * 1024]),
        tcp::SocketBuffer::new(vec![0; 64 * 1024]),
    );
    socket.set_keep_alive(Some(smoltcp::time::Duration::from_secs(KEEPALIVE.as_secs())));
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(socket);
    sockets
        .get_mut::<tcp::Socket>(handle)
        .connect(iface.context(), (IpAddress::Ipv6(remote.0), remote.1), local_addr.1)
        .map_err(|e| FetchError::other(format!("connect: {e}")))?;

    let where_ = format!("[{}]:{}", remote.0, remote.1);
    let connect_deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut established = false;
    let mut unreachable = 0;
    // Local bytes waiting for room in the mesh socket's send buffer.
    let mut to_mesh: VecDeque<u8> = VecDeque::new();
    let mut local_closed = false;
    let mut buf = vec![0u8; 16 * 1024];

    loop {
        if !alive.load(Ordering::Relaxed) {
            return Err(FetchError::restarted());
        }
        if stop.load(Ordering::Relaxed) {
            sockets.get_mut::<tcp::Socket>(handle).abort();
            iface.poll(now(), &mut device, &mut sockets);
            return Ok(());
        }
        iface.poll(now(), &mut device, &mut sockets);
        if let Some(e) = device.failed.take() {
            return Err(e);
        }
        let socket = sockets.get_mut::<tcp::Socket>(handle);
        if socket.is_active() && socket.state() != tcp::State::SynSent {
            established = true;
        }
        // Local → mesh.
        while !to_mesh.is_empty() && socket.can_send() {
            let (a, _) = to_mesh.as_slices();
            let n = socket.send_slice(a).unwrap_or(0);
            if n == 0 {
                break;
            }
            to_mesh.drain(..n);
        }
        if local_closed && to_mesh.is_empty() {
            socket.close();
        }
        // Mesh → local. The write blocks on a full loopback buffer, which is
        // the backpressure: the mesh window closes behind it.
        let mut wrote = 0;
        while socket.can_recv() && wrote < LOCAL_BACKLOG {
            let n = socket.recv_slice(&mut buf).unwrap_or(0);
            if n == 0 {
                break;
            }
            if local.write_all(&buf[..n]).is_err() {
                socket.abort();
                iface.poll(now(), &mut device, &mut sockets);
                return Ok(());
            }
            wrote += n;
        }
        let socket = sockets.get_mut::<tcp::Socket>(handle);
        if !socket.is_open() || (established && !socket.may_recv() && !socket.can_recv()) {
            // The far side closed, or both directions are done.
            return if established {
                Ok(())
            } else {
                Err(FetchError::unreachable(format!(
                    "{where_} refused the connection (nothing listening on that port?)"
                )))
            };
        }
        if !established && Instant::now() >= connect_deadline {
            return Err(FetchError::unreachable(if unreachable > 0 {
                format!("no route to {where_} over the mesh")
            } else {
                format!("{where_} did not answer (is the node online?)")
            }));
        }

        let wait = iface
            .poll_delay(now(), &sockets)
            .map(|d| Duration::from_micros(d.total_micros()))
            .unwrap_or(IDLE_WAIT)
            .min(IDLE_WAIT);
        let mut handle_event = |ev: Event, device: &mut Device<E>| match ev {
            Event::Mesh(p) if is_icmp_unreachable(&p) => unreachable += 1,
            Event::Mesh(p) => device.rx.push_back(p),
            Event::Local(bytes) => to_mesh.extend(bytes),
            Event::LocalClosed => local_closed = true,
        };
        match events.recv_timeout(wait) {
            Ok(ev) => {
                handle_event(ev, &mut device);
                while let Ok(ev) = events.try_recv() {
                    handle_event(ev, &mut device);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            // Both feeders gone: the flow was torn down and the local side
            // is closed.
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
        if unreachable >= 3 && !established {
            return Err(FetchError::unreachable(format!(
                "no route to {where_} over the mesh (the node does not know a path to it)"
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mesh_relay_urls() {
        let npub = pubdom_core::Npub::from_bytes([7; 32]).to_string();
        assert_eq!(
            parse_mesh_relay(&format!("ws://{npub}.fips:7777/nostr")),
            Some((npub.clone(), 7777, "/nostr".into()))
        );
        assert_eq!(parse_mesh_relay(&format!("ws://{}.FIPS", npub.to_uppercase())), Some((npub.clone(), 80, String::new())));
        assert_eq!(parse_mesh_relay(&format!("wss://{npub}.fips")), None, "no TLS on the mesh");
        assert_eq!(parse_mesh_relay("ws://relay.example.org"), None);
        assert_eq!(parse_mesh_relay("ws://[fd00::1]:80"), None);
        assert_eq!(parse_mesh_relay(&format!("ws://{npub}.fips:0")), None);
    }

    /// The pipe between a real loopback socket and a smoltcp echo server on
    /// the far end of two channels: bytes both ways, then a clean close
    /// started from the local side.
    #[test]
    fn pipe_against_a_userspace_echo_server() {
        let client_addr: Ipv6Addr = "fd00::1".parse().unwrap();
        let server_addr: Ipv6Addr = "fd00::2".parse().unwrap();
        let (to_server, server_rx) = mpsc::channel::<Vec<u8>>();
        let (events_tx, events) = mpsc::channel::<Event>();

        let to_client = events_tx.clone();
        let server = std::thread::spawn(move || {
            let start = Instant::now();
            let now = || smoltcp::time::Instant::from_millis(start.elapsed().as_millis() as i64);
            let mut device = Device {
                rx: VecDeque::new(),
                egress: move |p: Vec<u8>| {
                    let _ = to_client.send(Event::Mesh(p));
                    Ok(())
                },
                failed: None,
            };
            let mut iface = Interface::new(Config::new(HardwareAddress::Ip), &mut device, now());
            iface.update_ip_addrs(|a| {
                let _ = a.push(IpCidr::new(IpAddress::Ipv6(server_addr), 8));
            });
            let mut sockets = SocketSet::new(Vec::new());
            let mut sock = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; 8192]),
                tcp::SocketBuffer::new(vec![0; 8192]),
            );
            sock.listen(7777).unwrap();
            let h = sockets.add(sock);
            let mut echoed = 0;
            while start.elapsed() < Duration::from_secs(10) {
                while let Ok(p) = server_rx.try_recv() {
                    device.rx.push_back(p);
                }
                iface.poll(now(), &mut device, &mut sockets);
                let s = sockets.get_mut::<tcp::Socket>(h);
                let mut b = [0u8; 1024];
                while let Ok(n @ 1..) = s.recv_slice(&mut b) {
                    s.send_slice(&b[..n]).unwrap();
                    echoed += n;
                }
                // The client finished sending: close our side too.
                if echoed > 0 && !s.may_recv() && s.state() == tcp::State::CloseWait {
                    s.close();
                }
                if echoed > 0 && !s.is_open() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            echoed
        });

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut app = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (piped, _) = listener.accept().unwrap();
        {
            let mut reader = piped.try_clone().unwrap();
            let tx = events_tx.clone();
            std::thread::spawn(move || {
                let mut buf = [0u8; 1024];
                loop {
                    match reader.read(&mut buf) {
                        Ok(0) | Err(_) => {
                            let _ = tx.send(Event::LocalClosed);
                            break;
                        }
                        Ok(n) => {
                            let _ = tx.send(Event::Local(buf[..n].to_vec()));
                        }
                    }
                }
            });
        }
        drop(events_tx);
        let pipe = std::thread::spawn(move || {
            run(
                move |p| {
                    let _ = to_server.send(p);
                    Ok(())
                },
                &events,
                (client_addr, 61234),
                (server_addr, 7777),
                &piped,
                &AtomicBool::new(true),
                &AtomicBool::new(false),
            )
        });

        app.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let msg: &[u8] = br#"["REQ","x",{}]"#;
        app.write_all(msg).unwrap();
        let mut got = vec![0u8; msg.len()];
        app.read_exact(&mut got).unwrap();
        assert_eq!(got, msg);
        // Local close: FIN over the mesh, the server closes, the pipe ends.
        app.shutdown(Shutdown::Write).unwrap();
        assert!(pipe.join().unwrap().is_ok());
        assert_eq!(server.join().unwrap(), msg.len());
    }
}
