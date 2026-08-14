//! Integration tests: refcounted fan-out + bounded overflow.

use bytes::Bytes;
use rithmic_gateway::subscriptions::{
    ClientId, ClientQueue, ClientQueueError, FanoutHub, SubKey,
};

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
    let n = hub.publish(&key, Bytes::from_static(b"tick")).await;
    assert_eq!(n, 1);
    let got = q.recv().await.expect("msg");
    assert_eq!(&got[..], b"tick");
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
        let _ = hub.publish(&key, Bytes::from(vec![i])).await;
    }
    let err = q.recv().await.expect_err("overflow");
    assert_eq!(err, ClientQueueError::Overflow);
}
