//! Test-only support (feature `testing`): a control server for the fake-plant
//! gateway binary and builders for scripted plant events.
//!
//! None of this is reachable from the shipped `rithmic-gateway` binary, which
//! has no switch to select a fake plant.

use std::sync::Arc;
use std::time::Duration;

use rithmic_plants::dto::{
    AccountPnlDto, BboDto, HistoryBarDto, LastTradeDto, OrderNotificationDto, PlantEvent,
};
use rithmic_plants::fake::{FakeFailure, FakeSession, Stream};
use rithmic_plants::AccountTriple;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

pub fn last_trade(symbol: &str, exchange: &str, price: f64, size: i32) -> PlantEvent {
    PlantEvent::LastTrade(LastTradeDto {
        symbol: Some(symbol.into()),
        exchange: Some(exchange.into()),
        trade_price: Some(price),
        trade_size: Some(size),
        aggressor: Some(1),
        ssboe: Some(1_700_000_000),
        usecs: Some(0),
        ts_event_ns: Some(1_700_000_000_000_000_000),
        is_snapshot: Some(false),
    })
}

pub fn bbo(
    symbol: &str,
    exchange: &str,
    bid: f64,
    bid_size: i32,
    ask: f64,
    ask_size: i32,
) -> PlantEvent {
    PlantEvent::Bbo(BboDto {
        symbol: Some(symbol.into()),
        exchange: Some(exchange.into()),
        bid_price: Some(bid),
        bid_size: Some(bid_size),
        ask_price: Some(ask),
        ask_size: Some(ask_size),
        ssboe: Some(1_700_000_000),
        usecs: Some(0),
        ts_event_ns: Some(1_700_000_000_000_000_000),
        is_snapshot: Some(false),
    })
}

pub fn time_bar(symbol: &str, exchange: &str, close: f64) -> PlantEvent {
    PlantEvent::TimeBar(HistoryBarDto {
        symbol: Some(symbol.into()),
        exchange: Some(exchange.into()),
        bar_type: Some(2),
        period: Some("1".into()),
        marker: Some(1_700_000_060),
        open_price: Some(close),
        high_price: Some(close),
        low_price: Some(close),
        close_price: Some(close),
        volume: Some(10),
        num_trades: Some(3),
        bid_volume: Some(4),
        ask_volume: Some(6),
        ts_event_ns: Some(1_700_000_060_000_000_000),
    })
}

pub fn account_pnl(account_id: &str, day_pnl: &str) -> PlantEvent {
    PlantEvent::AccountPnl(AccountPnlDto {
        account_id: Some(account_id.into()),
        fcm_id: None,
        ib_id: None,
        account_balance: None,
        cash_on_hand: None,
        margin_balance: None,
        day_pnl: Some(day_pnl.into()),
        open_position_pnl: None,
        closed_position_pnl: None,
        available_buying_power: None,
        used_buying_power: None,
        is_snapshot: Some(false),
        ssboe: Some(1_700_000_000),
        usecs: Some(0),
    })
}

pub fn order_notification(kind: &str, basket_id: &str, user_tag: &str, symbol: &str) -> PlantEvent {
    PlantEvent::OrderNotification(OrderNotificationDto {
        source: "rithmic".into(),
        kind: Some(kind.into()),
        notify_type: None,
        notify_type_name: None,
        status: None,
        basket_id: Some(basket_id.into()),
        exchange_order_id: None,
        user_tag: Some(user_tag.into()),
        account_id: Some("ACCT".into()),
        symbol: Some(symbol.into()),
        exchange: Some("CME".into()),
        quantity: Some(1),
        total_fill_size: None,
        total_unfilled_size: None,
        fill_size: None,
        price: Some(100.0),
        trigger_price: None,
        avg_fill_price: None,
        fill_price: None,
        transaction_type: None,
        price_type: None,
        duration: None,
        fill_id: None,
        text: None,
        report_text: None,
        completion_reason: None,
        ssboe: Some(1_700_000_000),
        usecs: Some(0),
        ts_event_ns: Some(1_700_000_000_000_000_000),
        is_snapshot: Some(false),
    })
}

/// Serve the line-based control protocol on `listener` until it is dropped.
///
/// One command per line; every reply ends with a line containing only `END`.
/// Commands: `ping`, `calls`, `clear_calls`, `drop_plants`,
/// `fail_next <method> <rithmic|closed|config> [message]`,
/// `push last_trade <sym> <exch> <price> <size>`,
/// `push bbo <sym> <exch> <bid> <bid_size> <ask> <ask_size>`,
/// `push time_bar <sym> <exch> <close>`, `push account_pnl <acct> <day_pnl>`,
/// `push order <kind> <basket_id> <user_tag> <symbol>`,
/// `push gap <ticker|history|pnl|order> <skipped>` (a scripted plant lag),
/// `set_account <account_id> <fcm_id> <ib_id>`.
pub async fn serve_control(listener: UnixListener, fake: Arc<FakeSession>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let fake = Arc::clone(&fake);
        tokio::spawn(async move {
            let _ = handle_control(stream, fake).await;
        });
    }
}

async fn handle_control(stream: UnixStream, fake: Arc<FakeSession>) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await? {
        let reply = run_command(&fake, line.trim());
        write.write_all(reply.as_bytes()).await?;
        write.write_all(b"END\n").await?;
    }
    Ok(())
}

fn run_command(fake: &FakeSession, line: &str) -> String {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["ping"] => "ok\n".into(),
        ["calls"] => fake
            .calls()
            .into_iter()
            .map(|c| format!("{c}\n"))
            .collect::<String>(),
        ["clear_calls"] => {
            fake.clear_calls();
            "ok\n".into()
        }
        ["drop_plants"] => {
            fake.drop_plants();
            "ok\n".into()
        }
        ["fail_next", method, kind, rest @ ..] => {
            let message = if rest.is_empty() {
                "injected".to_string()
            } else {
                rest.join(" ")
            };
            let failure = match *kind {
                "rithmic" => FakeFailure::Rithmic(message),
                "closed" => FakeFailure::ChannelClosed("ticker"),
                "config" => FakeFailure::Config(message),
                other => return format!("err unknown failure kind {other}\n"),
            };
            fake.fail_next(method, failure);
            "ok\n".into()
        }
        ["push", "last_trade", sym, exch, price, size] => push(
            fake,
            Stream::Ticker,
            parse2(price, size).map(|(p, s)| last_trade(sym, exch, p, s as i32)),
        ),
        ["push", "bbo", sym, exch, bid, bid_size, ask, ask_size] => {
            let parsed = (|| {
                Some(bbo(
                    sym,
                    exch,
                    bid.parse().ok()?,
                    bid_size.parse().ok()?,
                    ask.parse().ok()?,
                    ask_size.parse().ok()?,
                ))
            })();
            push(fake, Stream::Ticker, parsed)
        }
        ["push", "time_bar", sym, exch, close] => push(
            fake,
            Stream::History,
            close.parse().ok().map(|c| time_bar(sym, exch, c)),
        ),
        ["push", "account_pnl", acct, day_pnl] => {
            push(fake, Stream::Pnl, Some(account_pnl(acct, day_pnl)))
        }
        ["push", "order", kind, basket, tag, sym] => push(
            fake,
            Stream::Order,
            Some(order_notification(kind, basket, tag, sym)),
        ),
        ["push", "gap", stream, skipped] => {
            match (
                rithmic_plants::Stream::from_plant(stream),
                skipped.parse::<u64>(),
            ) {
                (Some(stream), Ok(skipped)) => {
                    fake.push_event(
                        stream,
                        PlantEvent::Gap(rithmic_plants::GapDto { stream, skipped }),
                    );
                    "ok\n".into()
                }
                _ => "err bad gap\n".into(),
            }
        }
        ["set_account", account_id, fcm_id, ib_id] => {
            fake.set_account(AccountTriple {
                account_id: (*account_id).into(),
                fcm_id: (*fcm_id).into(),
                ib_id: (*ib_id).into(),
            });
            "ok\n".into()
        }
        _ => format!("err unknown command: {line}\n"),
    }
}

fn parse2(a: &str, b: &str) -> Option<(f64, f64)> {
    Some((a.parse().ok()?, b.parse().ok()?))
}

fn push(fake: &FakeSession, stream: Stream, event: Option<PlantEvent>) -> String {
    match event {
        Some(event) => {
            fake.push_event(stream, event);
            "ok\n".into()
        }
        None => "err bad number\n".into(),
    }
}

/// Wait helper for tests: poll `cond` until true or `timeout`.
pub async fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_commands_script_the_fake() {
        let fake = FakeSession::new();
        assert_eq!(run_command(&fake, "ping"), "ok\n");
        assert_eq!(
            run_command(&fake, "push last_trade NQH6 CME 100.25 2"),
            "ok\n"
        );
        assert_eq!(
            run_command(&fake, "push last_trade NQH6 CME x 2"),
            "err bad number\n"
        );
        assert_eq!(run_command(&fake, "set_account A F I"), "ok\n");
        assert!(run_command(&fake, "nope").starts_with("err unknown command"));
        assert!(run_command(&fake, "fail_next subscribe bogus").starts_with("err unknown failure"));
        assert_eq!(
            run_command(&fake, "fail_next subscribe rithmic boom now"),
            "ok\n"
        );
    }

    #[test]
    fn calls_reply_lists_one_call_per_line() {
        let fake = FakeSession::new();
        assert_eq!(run_command(&fake, "calls"), "");
    }
}
