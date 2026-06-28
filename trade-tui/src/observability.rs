use anyhow::Result;
use opentelemetry::trace::{TraceContextExt, Tracer};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use trade_core::AppState;

pub struct OtelTelemetry {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl OtelTelemetry {
    pub fn init_stdout(service_name: &str) -> Self {
        let resource = Resource::builder()
            .with_service_name(service_name.to_string())
            .build();

        let tracer_provider = SdkTracerProvider::builder()
            .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
            .with_resource(resource.clone())
            .build();
        global::set_tracer_provider(tracer_provider.clone());

        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(opentelemetry_stdout::MetricExporter::default())
            .with_resource(resource)
            .build();
        global::set_meter_provider(meter_provider.clone());

        Self {
            tracer_provider,
            meter_provider,
        }
    }

    pub fn emit_state_snapshot(&self, state: &AppState, replay: bool) {
        let tracer = global::tracer("trade-tui");
        tracer.in_span("trade_tui.state_projection", |cx| {
            let span = cx.span();
            span.set_attribute(KeyValue::new("cockpit.replay", replay));
            span.set_attribute(KeyValue::new(
                "cockpit.source",
                state.connection.nats.clone(),
            ));
            span.set_attribute(KeyValue::new(
                "cockpit.account_id",
                state.account.account_id.clone(),
            ));
            span.set_attribute(KeyValue::new(
                "cockpit.last_event_sequence",
                state.connection.last_event_sequence.unwrap_or_default() as i64,
            ));
            span.add_event(
                "projection_loaded",
                vec![
                    KeyValue::new("strategies", state.strategies.by_id.len() as i64),
                    KeyValue::new("orders", state.orders.by_correlation_id.len() as i64),
                    KeyValue::new("positions", state.positions.by_key.len() as i64),
                    KeyValue::new("open_alerts", state.alerts.open_count() as i64),
                ],
            );
        });

        let meter = global::meter("trade-tui");
        meter.u64_counter("tui_events_ingested_total").build().add(
            state.connection.events_ingested,
            &[KeyValue::new("source", state.connection.nats.clone())],
        );
        meter.u64_counter("tui_events_coalesced_total").build().add(
            state.connection.events_coalesced,
            &[KeyValue::new("source", state.connection.nats.clone())],
        );
        meter
            .u64_counter("tui_dropped_market_updates_total")
            .build()
            .add(
                state.connection.dropped_market_updates,
                &[KeyValue::new("source", state.connection.nats.clone())],
            );
        meter
            .u64_observable_gauge("tui_audit_events_retained")
            .with_callback({
                let retained = state.connection.audit_events_retained as u64;
                move |observer| observer.observe(retained, &[])
            })
            .build();
    }

    pub fn shutdown(self) -> Result<()> {
        self.tracer_provider
            .shutdown()
            .map_err(|error| anyhow::anyhow!("otel trace shutdown failed: {error}"))?;
        self.meter_provider
            .shutdown()
            .map_err(|error| anyhow::anyhow!("otel metrics shutdown failed: {error}"))?;
        Ok(())
    }
}
