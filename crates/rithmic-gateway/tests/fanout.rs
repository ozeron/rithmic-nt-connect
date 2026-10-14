//! Integration tests: refcounted fan-out + bounded overflow.

use bytes::Bytes;
use rithmic_gateway::subscriptions::{
    ClientId, ClientQueue, ClientQueueError, Fan, FanoutHub, Kind, SubKey,
};

fn fan(bytes: Bytes) -> Fan {
    Fan {
        kind: Kind::Any,
        bytes,
    }
}

#[tokio::test]
async fn refcount_zero_to_one_needs_venue() {
    let hub = FanoutHub::new(8);
    let key = SubKey {
        symbol: "NQ".into(),
        exchange: "CME".into(),
    };
    assert!(hub.add_interest(key.clone()).await);
    assert!(!hub.add_interest(key.clone()).await);
    assert!(!hub.remove_interest(&key).await);
    assert!(hub.remove_interest(&key).await);
}

#[tokio::test]
async fn publish_reaches_subscriber() {
    let hub = FanoutHub::new(8);
    let key = SubKey {
        symbol: "ES".into(),
        exchange: "CME".into(),
    };
    assert!(hub.add_interest(key.clone()).await);
    let rx = hub.subscribe_receiver(&key).await.expect("rx");
    let mut q = ClientQueue::from_receiver(ClientId::new(), rx);
    let n = hub.publish(&key, fan(Bytes::from_static(b"tick"))).await;
    assert_eq!(n, 1);
    let got = q.recv().await.expect("msg");
    assert_eq!(&got.bytes[..], b"tick");
}

#[tokio::test]
async fn lagging_client_gets_overflow() {
    let hub = FanoutHub::new(2);
    let key = SubKey {
        symbol: "YM".into(),
        exchange: "CBOT".into(),
    };
    assert!(hub.add_interest(key.clone()).await);
    let rx = hub.subscribe_receiver(&key).await.expect("rx");
    let mut q = ClientQueue::from_receiver(ClientId::new(), rx);
    // Flood beyond capacity without receiving.
    for i in 0..16u8 {
        let _ = hub.publish(&key, fan(Bytes::from(vec![i]))).await;
    }
    let err = q.recv().await.expect_err("overflow");
    assert!(
        matches!(err, ClientQueueError::Overflow { skipped } if skipped > 0),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_topic_with_no_attached_client_has_no_live_sender() {
    let hub = FanoutHub::new(8);
    let key = SubKey {
        symbol: "NQ".into(),
        exchange: "CME".into(),
    };
    // Unknown topic, then a topic with interest but no receiver: nothing to send to.
    assert!(hub.live_sender("NQ", "CME").await.is_none());
    assert!(hub.add_interest(key.clone()).await);
    assert!(hub.live_sender("NQ", "CME").await.is_none());
    // Once a client's receiver is attached, the borrowed lookup finds it.
    let _rx = hub.subscribe_receiver(&key).await.expect("rx");
    assert!(hub.live_sender("NQ", "CME").await.is_some());
    assert!(hub.live_sender("NQ", "CBOT").await.is_none());
    assert!(hub.live_sender("ES", "CME").await.is_none());
    // Leaving removes the topic entirely.
    assert!(hub.remove_interest(&key).await);
    assert!(hub.live_sender("NQ", "CME").await.is_none());
}

#[tokio::test]
async fn gap_frames_reach_every_symbol_topic_but_not_sentinels() {
    let hub = FanoutHub::new(8);
    let nq = SubKey {
        symbol: "NQ".into(),
        exchange: "CME".into(),
    };
    let ym = SubKey {
        symbol: "YM".into(),
        exchange: "CBOT".into(),
    };
    let pnl = SubKey {
        symbol: "__pnl__".into(),
        exchange: String::new(),
    };
    for k in [&nq, &ym, &pnl] {
        hub.add_interest(k.clone()).await;
    }
    let mut queues = Vec::new();
    for k in [&nq, &ym, &pnl] {
        let rx = hub.subscribe_receiver(k).await.expect("rx");
        queues.push(ClientQueue::from_receiver(ClientId::new(), rx));
    }
    assert_eq!(
        hub.publish_symbols(fan(Bytes::from_static(b"gap"))).await,
        2
    );
    assert_eq!(&queues[0].recv().await.unwrap().bytes[..], b"gap");
    assert_eq!(&queues[1].recv().await.unwrap().bytes[..], b"gap");
    let sentinel =
        tokio::time::timeout(std::time::Duration::from_millis(50), queues[2].recv()).await;
    assert!(
        sentinel.is_err(),
        "the pnl sentinel topic gets no symbol gap"
    );
}
