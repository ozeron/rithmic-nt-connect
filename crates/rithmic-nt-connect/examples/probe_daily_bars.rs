//! Daily-only history-plant probe (no PnL, no orders).
//!
//! ```text
//! cargo run -p rithmic-nt-connect --example probe_daily_bars
//! ```
//!
//! Close MotiveWave / R|Trader first (one Rithmic session per login).

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rithmic_nt_connect::{
    PlantSet, RithmicSession, SessionConfig, SessionConfigBuilder, TimeBarProbeRow,
};
use rithmic_rs::TimeBarType;

fn load_dotenv(path: &Path) {
    let Ok(text) = fs::read_to_string(path) else {
        return;
    };
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || !line.contains('=') {
            continue;
        }
        let (k, v) = line.split_once('=').expect("contains =");
        let key = k.trim();
        let val = v.trim().trim_matches('"').trim_matches('\'');
        if env::var_os(key).is_none() {
            env::set_var(key, val);
        }
    }
}

fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| env::var(k).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn config_from_env() -> Result<SessionConfig, Box<dyn std::error::Error>> {
    let user = env_first(&["RITHMIC_USER", "RITHMIC_USERNAME", "RHITMIC_USERNAME"])
        .ok_or("missing RITHMIC_USER")?;
    let password = env_first(&["RITHMIC_PASSWORD", "RHITMIC_PASSWORD"])
        .ok_or("missing RITHMIC_PASSWORD")?;
    let mut b = SessionConfigBuilder::default().user(user).password(password);
    if let Some(system) = env_first(&["RITHMIC_SYSTEM", "RITHMIC_SYSTEM_NAME", "RITHMIC_LIVE_SYSTEM_NAME"])
    {
        b = b.system_name(system);
    }
    if let Some(url) = env_first(&["RITHMIC_GATEWAY", "RITHMIC_LIVE_URL", "RITHMIC_URL"]) {
        b = b.url(url);
    }
    if let Some(app) = env_first(&["RITHMIC_APP_NAME"]) {
        b = b.app_name(app);
    }
    Ok(b.build()?)
}

fn yyyymmdd_utc(unix_sec: i64) -> i32 {
    let days = unix_sec.div_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + i64::from(m <= 2);
    (y * 10_000 + m * 100 + d) as i32
}

fn summarize(label: &str, start: i32, end: i32, rows: &[TimeBarProbeRow]) {
    let mut variants: BTreeMap<&str, usize> = BTreeMap::new();
    let mut skips: BTreeMap<String, usize> = BTreeMap::new();
    let mut rp: BTreeMap<String, usize> = BTreeMap::new();
    let mut parsed = 0usize;
    for row in rows {
        *variants.entry(row.variant.as_str()).or_default() += 1;
        if row.parsed {
            parsed += 1;
        }
        if let Some(reason) = &row.skip_reason {
            *skips.entry(reason.clone()).or_default() += 1;
        }
        let code = if row.rp_code.is_empty() {
            "(empty)".to_string()
        } else {
            row.rp_code.join("|")
        };
        *rp.entry(code).or_default() += 1;
    }
    println!();
    println!("=== {label} start={start} end={end} raw={} parsed={parsed} ===", rows.len());
    println!("variants: {variants:?}");
    println!("rp_code:  {rp:?}");
    println!("skipped:  {skips:?}");
    let show = rows.iter().take(8).chain(rows.iter().rev().take(3).rev());
    for (i, row) in show.enumerate() {
        println!(
            "  [{i}] var={} parsed={} skip={:?} rp={:?} err={:?} type={:?} period={:?} marker={:?} o={:?} h={:?} l={:?} c={:?} settle={:?} has_settle={:?} vol={:?} trades={:?} {}/{}",
            row.variant,
            row.parsed,
            row.skip_reason,
            row.rp_code,
            row.error,
            row.bar_type,
            row.period,
            row.marker,
            row.open_price,
            row.high_price,
            row.low_price,
            row.close_price,
            row.settlement_price,
            row.has_settlement_price,
            row.volume,
            row.num_trades,
            row.symbol.as_deref().unwrap_or("-"),
            row.exchange.as_deref().unwrap_or("-"),
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    load_dotenv(&manifest.join("../../.env"));
    if let Ok(extra) = env::var("RITHMIC_CONNECT_DOTENV") {
        for part in extra.split(if cfg!(windows) { ';' } else { ':' }) {
            if !part.is_empty() {
                load_dotenv(Path::new(part));
            }
        }
    }

    let cfg = config_from_env()?;
    println!(
        "connecting system={} url={} user={}*** plants=market_data (no pnl)",
        cfg.system_name(),
        cfg.url(),
        &cfg.user()[..cfg.user().len().min(2)],
    );
    println!("NOTE: close MotiveWave / R|Trader first (one session per login).");

    let mut session = RithmicSession::with_plants(cfg, PlantSet::MARKET_DATA);
    session.connect().await?;

    let root = env_first(&["RITHMIC_SYMBOL", "SYMBOL"]).unwrap_or_else(|| "NQ".into());
    let exchange = env_first(&["RITHMIC_EXCHANGE", "EXCHANGE"]).unwrap_or_else(|| "CME".into());
    let front = session.get_front_month(&root, &exchange).await?;
    let symbol = front
        .trading_symbol
        .clone()
        .or(front.symbol.clone())
        .unwrap_or(root);
    println!(
        "front month trading_symbol={symbol} trading_exchange={:?} listed={:?}",
        front.trading_exchange, front.symbol
    );

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let unix_end = i32::try_from(now)?;
    let unix_start = i32::try_from(now - 40 * 86_400)?;
    let ymd_end = yyyymmdd_utc(now);
    let ymd_start = yyyymmdd_utc(now - 40 * 86_400);

    let t0 = SystemTime::now();
    let unix_rows = session
        .probe_time_bars(&symbol, &exchange, TimeBarType::DailyBar, 1, unix_start, unix_end)
        .await?;
    let unix_ms = t0.elapsed().unwrap_or_default().as_millis();
    summarize(&format!("DailyBar period=1 unix ({unix_ms}ms)"), unix_start, unix_end, &unix_rows);

    let t1 = SystemTime::now();
    let ymd_rows = session
        .probe_time_bars(&symbol, &exchange, TimeBarType::DailyBar, 1, ymd_start, ymd_end)
        .await?;
    let ymd_ms = t1.elapsed().unwrap_or_default().as_millis();
    summarize(
        &format!("DailyBar period=1 YYYYMMDD ({ymd_ms}ms)"),
        ymd_start,
        ymd_end,
        &ymd_rows,
    );

    let loaded = session
        .load_time_bars_all(&symbol, &exchange, TimeBarType::DailyBar, 1, unix_start, unix_end)
        .await?;
    println!();
    println!(
        "load_time_bars_all unix window → {} bars",
        loaded.len()
    );
    if let (Some(first), Some(last)) = (loaded.first(), loaded.last()) {
        println!(
            "  first marker={:?} ts_event_ns={:?} close={:?}",
            first.marker, first.ts_event_ns, first.close_price
        );
        println!(
            "  last  marker={:?} ts_event_ns={:?} close={:?}",
            last.marker, last.ts_event_ns, last.close_price
        );
    }

    session.disconnect().await?;
    println!();
    println!(
        "RESULT daily_unix raw={} parsed={} daily_yyyymmdd raw={} parsed={} load_all={}",
        unix_rows.len(),
        unix_rows.iter().filter(|r| r.parsed).count(),
        ymd_rows.len(),
        ymd_rows.iter().filter(|r| r.parsed).count(),
        loaded.len(),
    );
    Ok(())
}
