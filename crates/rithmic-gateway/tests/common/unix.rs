//! Process restart coverage keeps real Unix listen/control sockets and flock.
use super::*;
use rithmic_gateway::singleton::flock_held;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

pub struct UnixParent {
    child: Option<Child>,
    pub dir: tempfile::TempDir,
    pub user: String,
    pub listen: PathBuf,
    pub control: PathBuf,
    opts: Opts,
}

impl UnixParent {
    pub async fn start(opts: Opts) -> Self {
        let mut parent = Self {
            child: None,
            dir: short_dir(),
            user: unique_user("fake"),
            listen: PathBuf::new(),
            control: PathBuf::new(),
            opts,
        };
        parent.listen = parent.dir.path().join("gw.sock");
        parent.control = parent.dir.path().join("ctl.sock");
        parent.spawn().await;
        parent
    }

    pub fn config(&self) -> GatewayConfig {
        GatewayConfig::new(&self.user, SYSTEM, URL, "Live")
            .with_listen(&format!("unix://{}", self.listen.display()))
            .expect("listen")
    }

    async fn spawn(&mut self) {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rithmic-gateway-fake"));
        cmd.env_clear()
            .env("RITHMIC_USER", &self.user)
            .env("RITHMIC_SYSTEM_NAME", SYSTEM)
            .env("RITHMIC_URL", URL)
            .env("RITHMIC_ENV", "Live")
            .env(
                "RITHMIC_GATEWAY_LISTEN",
                format!("unix://{}", self.listen.display()),
            )
            .env("RITHMIC_FAKE_CONTROL", &self.control)
            .env("RITHMIC_GATEWAY_RESTORE_BACKOFF_MS", "50")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // The credential flock lives under the runtime dir both sides derive.
        for key in ["PATH", "HOME", "TMPDIR", "XDG_RUNTIME_DIR"] {
            if let Ok(v) = std::env::var(key) {
                cmd.env(key, v);
            }
        }
        if self.opts.trading {
            cmd.env("RITHMIC_ENABLE_TRADING", "1");
        }
        if self.opts.cancel_all {
            cmd.env("RITHMIC_GATEWAY_CANCEL_ALL", "1");
        }
        self.child = Some(cmd.spawn().expect("spawn fake gateway"));
        let up = wait_until(Duration::from_secs(20), || {
            std::os::unix::net::UnixStream::connect(&self.listen).is_ok()
                && flock_held(&self.user, SYSTEM, URL, "Live")
                // The socket file exists from `bind`, before it listens: wait for
                // a connect to succeed, not for the file.
                && std::os::unix::net::UnixStream::connect(&self.control).is_ok()
        })
        .await;
        assert!(up, "fake gateway did not come up");
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Kill the parent and start a fresh one on the same paths / fingerprint.
    pub async fn restart(&mut self) {
        self.stop();
        assert!(
            wait_until(Duration::from_secs(10), || !flock_held(
                &self.user, SYSTEM, URL, "Live"
            ))
            .await,
            "flock not released"
        );
        let _ = std::fs::remove_file(&self.listen);
        self.spawn().await;
    }

    pub async fn control(&self, commands: &[String]) -> Vec<Vec<String>> {
        let mut stream = UnixStream::connect(&self.control)
            .await
            .expect("control connect");
        let mut replies = Vec::new();
        for chunk in commands.chunks(200) {
            let mut text = String::new();
            for c in chunk {
                text.push_str(c);
                text.push('\n');
            }
            stream
                .write_all(text.as_bytes())
                .await
                .expect("control write");
            let mut lines = BufReader::new(&mut stream).lines();
            let mut current = Vec::new();
            let mut got = 0;
            while got < chunk.len() {
                let line = lines
                    .next_line()
                    .await
                    .expect("control read")
                    .expect("control closed");
                if line == "END" {
                    replies.push(std::mem::take(&mut current));
                    got += 1;
                } else {
                    current.push(line);
                }
            }
        }
        replies
    }

    pub async fn command(&self, cmd: &str) -> Vec<String> {
        let reply = self.control(&[cmd.to_string()]).await.pop().expect("reply");
        assert!(
            !reply.first().is_some_and(|l| l.starts_with("err")),
            "{cmd}: {reply:?}"
        );
        reply
    }

    pub async fn calls(&self) -> Vec<String> {
        self.command("calls").await
    }

    pub async fn count(&self, prefix: &str) -> usize {
        self.calls()
            .await
            .iter()
            .filter(|c| c.starts_with(prefix))
            .count()
    }

    pub async fn clear_calls(&self) {
        self.command("clear_calls").await;
    }

    pub async fn push_last_trades(&self, symbol: &str, n: usize) {
        let cmds: Vec<String> = (0..n)
            .map(|i| format!("push last_trade {symbol} {CME} {}.25 1", 100 + i))
            .collect();
        for reply in self.control(&cmds).await {
            assert_eq!(reply, ["ok"]);
        }
    }

    pub async fn wait_for_call(&self, prefix: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.count(prefix).await > 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
}

impl Drop for UnixParent {
    fn drop(&mut self) {
        self.stop();
    }
}
