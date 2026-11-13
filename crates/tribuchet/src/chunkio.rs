//! Bridges between blocking std::io streams (NAR/tar/zstd codecs) and
//! async tokio channels carrying byte chunks.

use std::io::{self, Read};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::{Endpoint, Server};

pub const CHUNK_SIZE: usize = 1024 * 1024;

/// HTTP/2 flow-control windows. The 64 KiB h2 default caps a stream at
/// ~window/RTT, throttling NAR transfer (worst on the two-hop output
/// relay). Sized to keep several CHUNK_SIZE messages in flight.
pub const H2_STREAM_WINDOW: u32 = 16 * 1024 * 1024;
pub const H2_CONNECTION_WINDOW: u32 = 32 * 1024 * 1024;

/// How often a worker tells the hub its state on its Control, whose
/// silence the hub takes to mean the worker may be gone.
pub const HEARTBEAT: Duration = Duration::from_secs(30);

/// The hub's worker server. No HTTP/2 keepalive: a PING waits behind
/// all its end has sent, so it times out on a busy link as on a dead one.
/// What says a worker may be gone is the silence of its Control.
pub fn session_server() -> Server {
    Server::builder()
        .initial_stream_window_size(Some(H2_STREAM_WINDOW))
        .initial_connection_window_size(Some(H2_CONNECTION_WINDOW))
}

/// A worker's connections to the hub, one for its Control and one for
/// its Session. No HTTP/2 keepalive, as on the server; TCP keepalive
/// keeps an idle Session's path open.
pub fn session_endpoint(endpoint: Endpoint) -> Endpoint {
    endpoint
        .tcp_keepalive(Some(HEARTBEAT))
        .initial_stream_window_size(Some(H2_STREAM_WINDOW))
        .initial_connection_window_size(Some(H2_CONNECTION_WINDOW))
}

/// `Read` implementation over a tokio channel of byte chunks. Used from
/// blocking threads (`blocking_recv`); EOF when the sender is dropped.
pub struct ChannelReader {
    rx: mpsc::Receiver<Vec<u8>>,
    current: Vec<u8>,
    pos: usize,
}

impl ChannelReader {
    pub fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            current: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.pos == self.current.len() {
            match self.rx.blocking_recv() {
                Some(chunk) => {
                    self.current = chunk;
                    self.pos = 0;
                }
                None => return Ok(0),
            }
        }
        let n = (self.current.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}
