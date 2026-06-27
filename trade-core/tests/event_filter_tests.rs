use trade_core::EventFilter;

#[test]
fn filters_events_by_domain_dimensions() {
    let events = trade_core::sample::sample_events();

    assert_eq!(
        events
            .iter()
            .filter(|event| EventFilter {
                strategy_id: Some("open-scalp".to_string()),
                ..EventFilter::default()
            }
            .matches(event))
            .count(),
        4
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| EventFilter {
                symbol: Some("MU".to_string()),
                ..EventFilter::default()
            }
            .matches(event))
            .count(),
        4
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| EventFilter {
                order_id: Some("ord-demo-001".to_string()),
                ..EventFilter::default()
            }
            .matches(event))
            .count(),
        5
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| EventFilter {
                event_type: Some("SignalGenerated".to_string()),
                ..EventFilter::default()
            }
            .matches(event))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| EventFilter {
                severity: Some("WARN".to_string()),
                ..EventFilter::default()
            }
            .matches(event))
            .count(),
        1
    );
}

#[test]
fn filters_events_by_publish_timestamp_window() {
    let mut events = trade_core::sample::sample_events();
    events[0].publish_ts_ns = 100;
    events[1].publish_ts_ns = 200;
    events[2].publish_ts_ns = 300;

    let filter = EventFilter {
        from_ts_ns: Some(150),
        to_ts_ns: Some(250),
        ..EventFilter::default()
    };

    let matched = events
        .iter()
        .filter(|event| filter.matches(event))
        .collect::<Vec<_>>();

    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].publish_ts_ns, 200);
}

#[test]
fn summarizes_active_filters_for_ui_surfaces() {
    let filter = EventFilter {
        strategy_id: Some("open-scalp".to_string()),
        symbol: Some("MU".to_string()),
        from_ts_ns: Some(100),
        to_ts_ns: Some(200),
        ..EventFilter::default()
    };

    assert_eq!(
        filter.summary().as_deref(),
        Some("strategy=open-scalp symbol=MU from_ts_ns=100 to_ts_ns=200")
    );
}
