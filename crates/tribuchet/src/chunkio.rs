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

/// HTTP/2 keepalive of a worker session, on both ends: an end that has
/// read nothing from the other for the interval PINGs it, and drops the
/// connection unless the PONG is back within the timeout. Detects a
/// dead or half-open peer instead of waiting on its traffic.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// How often each end of a worker session sends the other something to
/// read, so that a live peer is never PINGed. A PING queues behind the
/// data its end already sent, up to H2_STREAM_WINDOW of it: on a link
/// slower than H2_STREAM_WINDOW / KEEPALIVE_TIMEOUT (0.84 MB/s) a busy
/// end's PING can miss the timeout and kill a live session. One message
/// per half interval reaches the peer within every interval with half
/// of one to spare for its own delivery.
pub const HEARTBEAT: Duration = Duration::from_secs(KEEPALIVE_INTERVAL.as_secs() / 2);

/// The hub's worker session server.
pub fn session_server() -> Server {
    Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .initial_stream_window_size(Some(H2_STREAM_WINDOW))
        .initial_connection_window_size(Some(H2_CONNECTION_WINDOW))
}

/// A worker's connection to the hub's session server.
pub fn session_endpoint(endpoint: Endpoint) -> Endpoint {
    endpoint
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
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
