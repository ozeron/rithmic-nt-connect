//! Stream ownership and connection factories, independent of Unix dialing.

use rithmic_host::{Error, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};

pub(super) trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<S: AsyncRead + AsyncWrite + Unpin + Send> Transport for S {}
pub(super) type Stream = Box<dyn Transport>;
type ConnectFuture = Pin<Box<dyn Future<Output = Result<Stream>> + Send>>;
pub(super) type Connector = Box<dyn Fn() -> ConnectFuture + Send + Sync>;

pub(super) enum Source {
    Unix,
    Once(Mutex<Option<Stream>>),
    Connector(Connector),
}

impl Source {
    pub(super) async fn connect(&self, cfg: &super::GatewayConfig) -> Result<Stream> {
        match self {
            Self::Unix => {
                let stream = super::attach::attach(cfg).await?;
                if cfg.attest_flock && !super::attach::parent_flock_held(cfg) {
                    return Err(Error::Gateway {
                        code: "parent_unattested".into(),
                        message: "listen path is up but credential flock is free — refusing impostor parent".into(),
                    });
                }
                Ok(Box::new(stream))
            }
            Self::Once(stream) => stream.lock().expect("transport").take().ok_or_else(|| {
                Error::Transport("supplied stream consumed; use a connector to reconnect".into())
            }),
            Self::Connector(connect) => connect().await,
        }
    }
}
