//! [`KafkaInput`]: the delivery loop over [`Consumer`], and the revoke and shutdown sequence of
//! `docs/adr/kafka-input.md` item 8.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_kafka::consumer::{Consumer, ConsumerSettings, Fetched, Next};
use logit_pipeline::{Fanout, Input, SendTimeout};
use tokio::sync::watch;
use tokio::time::Instant;

use super::decode::{Decoded, KafkaInFormat, RecordDecoder};

/// How long one attempt at a send waits before the loop checks for a revoke or a shutdown and
/// offers the batch again. A timed-out attempt sent nothing, so a retry is no duplicate.
const SEND_SLICE: Duration = Duration::from_millis(250);

/// Everything a [`KafkaInput`] is built from; `logit-cli` fills it from config.
#[derive(Clone)]
pub struct KafkaInSettings {
    pub consumer: ConsumerSettings,
    pub format: KafkaInFormat,
    /// `receive.batch_max_events`: at least 1.
    pub batch_max_events: usize,
    /// `receive.batch_max_bytes`, measured in record value bytes: at least 1.
    pub batch_max_bytes: usize,
    /// How long the batch in flight may wait once a revoke is pending.
    pub drain_timeout: Duration,
    /// `receive.shutdown_grace`. At shutdown the batch in flight waits at most half of it, with
    /// the other half left for the commit and the LeaveGroup.
    pub shutdown_grace: Duration,
}

impl KafkaInSettings {
    /// `consumer` read as `format`, with `receive:`'s defaults.
    pub fn new(consumer: ConsumerSettings, format: KafkaInFormat) -> Self {
        Self {
            consumer,
            format,
            batch_max_events: 1_000,
            batch_max_bytes: 1 << 20,
            drain_timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

/// One run of a [`Fetched`]'s records, decoded: what goes downstream together and is marked
/// accepted together.
struct Cut {
    /// Offsets this cut covers, from [`Fetched::cut`].
    range: Range<i64>,
    batches: Vec<EventBatch>,
}

/// How a delivery ended.
enum Delivery {
    /// Every cut accepted, or the rest deferred under a revoke.
    Done,
    /// Shutdown fired; the rest is deferred.
    Shutdown,
    /// No consumer took a batch.
    Closed,
}

/// `kafka_in`: consumes its topics in a group and sends their records downstream. See the
/// module doc of [`super`].
pub struct KafkaInput {
    settings: KafkaInSettings,
    telemetry: Telemetry,
    diag: Diagnostics,
    consumer: Option<Consumer>,
    decoder: RecordDecoder,
}

impl KafkaInput {
    pub fn new(settings: KafkaInSettings) -> Self {
        let decoder = RecordDecoder::new(settings.format.clone());
        Self {
            settings,
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            consumer: None,
            decoder,
        }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.decoder.set_telemetry(telemetry.clone());
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    fn count(&self, name: &'static str, n: usize, tags: &[(&'static str, &'static str)]) {
        if n > 0 {
            self.telemetry.count(name, n as f64, tags);
        }
    }

    /// Decodes every record of `fetched`, counting what it drops, skips, or omits, and cuts the
    /// events into runs bounded by `batch_max_events` and `batch_max_bytes`. A record is never
    /// split: one over a bound goes alone.
    fn decode(&mut self, fetched: &Fetched) -> Vec<Cut> {
        let now = crate::http::now_nanos();
        let tp = &fetched.tp;
        let mut cuts = Vec::new();
        let mut start = 0;
        let mut batches: Vec<EventBatch> = Vec::new();
        let (mut events, mut bytes) = (0, 0);
        let max_events = self.settings.batch_max_events.max(1);
        let max_bytes = self.settings.batch_max_bytes.max(1);
        for (i, record) in fetched.records.iter().enumerate() {
            let (decoded, omitted) = self.decoder.decode(&tp.topic, tp.partition, record, now);
            self.count("logit.input.headers.omitted", omitted.0, &[("reason", "invalid_name")]);
            let decoded = match decoded {
                Decoded::Batches(decoded) => decoded,
                Decoded::Skipped(reason) => {
                    self.count("logit.input.records.skipped", 1, &[("reason", reason)]);
                    continue;
                }
                Decoded::Undecodable(why) => {
                    self.count("logit.input.records.dropped", 1, &[("reason", "undecodable")]);
                    self.diag.clone().warn_throttled(
                        "records_dropped",
                        format_args!(
                            "Kafka {tp} offset {}: a record that doesn't decode as the configured \
                             format was dropped and committed past: {why}",
                            record.offset
                        ),
                    );
                    continue;
                }
            };
            let n: usize = decoded.iter().map(|b| b.events.len()).sum();
            let size = record.value.as_ref().map_or(0, |v| v.len());
            if events > 0 && (events + n > max_events || bytes + size > max_bytes) {
                cuts.push(Cut {
                    range: fetched.cut(start..i),
                    batches: std::mem::take(&mut batches),
                });
                start = i;
                (events, bytes) = (0, 0);
            }
            for batch in decoded {
                match batches.last_mut() {
                    Some(last) if same_group(last, &batch) => last.events.extend(batch.events),
                    _ => batches.push(batch),
                }
            }
            events += n;
            bytes += size;
        }
        cuts.push(Cut { range: fetched.cut(start..fetched.records.len()), batches });
        cuts
    }

    /// Sends each cut in order and marks it accepted once every batch of it is. While no revoke
    /// is pending, a batch that finds no room is offered again with no end; once one is pending,
    /// or shutdown fires, the batch in flight has until the drain deadline, and a batch not
    /// accepted by then is deferred with every batch behind it.
    async fn deliver(
        &mut self,
        fetched: Fetched,
        sink: &Fanout,
        shutdown: &watch::Receiver<bool>,
        drain: &mut Option<Instant>,
    ) -> Delivery {
        let cuts = self.decode(&fetched);
        let consumer = self.consumer.as_mut().expect("bound before delivery");
        let mut cuts = cuts.into_iter();
        while let Some(cut) = cuts.next() {
            let mut batches = cut.batches.into_iter();
            while let Some(mut batch) = batches.next() {
                loop {
                    if drain.is_none() {
                        if *shutdown.borrow() {
                            let grace = self.settings.shutdown_grace / 2;
                            *drain = Some(Instant::now() + self.settings.drain_timeout.min(grace));
                        } else if consumer.revoke_pending().is_some() {
                            *drain = Some(Instant::now() + self.settings.drain_timeout);
                        }
                    }
                    let slice = Instant::now() + SEND_SLICE;
                    let deadline = drain.map_or(slice, |d| d.min(slice));
                    match sink.send_with_deadline(batch, deadline).await {
                        Ok(true) => break,
                        Ok(false) => {
                            let behind = batches.len()
                                + cuts.by_ref().map(|c| c.batches.len()).sum::<usize>();
                            self.telemetry.count(
                                "logit.input.batches.dropped",
                                1.0,
                                &[("reason", "closed_consumer")],
                            );
                            if behind > 0 {
                                self.telemetry.count(
                                    "logit.input.batches.deferred",
                                    behind as f64,
                                    &[],
                                );
                            }
                            self.diag.clone().warn_throttled(
                                "closed_consumer",
                                format_args!(
                                    "no consumer took a batch from Kafka {}; the input stops, \
                                     and its partitions go to another member",
                                    fetched.tp
                                ),
                            );
                            return Delivery::Closed;
                        }
                        Err(SendTimeout(back)) => {
                            if drain.is_some_and(|d| Instant::now() >= d) {
                                let behind = 1
                                    + batches.len()
                                    + cuts.by_ref().map(|c| c.batches.len()).sum::<usize>();
                                self.telemetry.count(
                                    "logit.input.batches.deferred",
                                    behind as f64,
                                    &[],
                                );
                                return if *shutdown.borrow() {
                                    Delivery::Shutdown
                                } else {
                                    Delivery::Done
                                };
                            }
                            batch = back;
                        }
                    }
                }
            }
            consumer.mark_accepted(&fetched.tp, fetched.epoch, cut.range);
        }
        if *shutdown.borrow() {
            Delivery::Shutdown
        } else {
            Delivery::Done
        }
    }
}

/// Whether `next` can join `last`: the same resource and scope.
fn same_group(last: &EventBatch, next: &EventBatch) -> bool {
    let resource = Arc::ptr_eq(&last.resource, &next.resource) || last.resource == next.resource;
    let scope = match (&last.scope, &next.scope) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b) || a == b,
        _ => false,
    };
    resource && scope
}

#[async_trait::async_trait]
impl Input for KafkaInput {
    /// Builds the consumer, which checks every bootstrap broker is a `host:port`; nothing is
    /// dialed until the run.
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.consumer.is_none() {
            let consumer = Consumer::new(
                self.settings.consumer.clone(),
                self.telemetry.clone(),
                self.diag.clone(),
            )?;
            self.consumer = Some(consumer);
        }
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    /// The delivery loop until shutdown, then the ADR's item 8 at shutdown: the batch in flight
    /// gets the drain deadline, then a commit and LeaveGroup. A closed consumer ends the same
    /// way.
    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.bind().await?;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let consumer = self.consumer.as_mut().expect("bound above");
            // A dropped sender ends the wait too: no signal can follow it.
            let next = tokio::select! {
                next = consumer.next() => next,
                _ = shutdown.wait_for(|&due| due) => break,
            };
            match next {
                Next::Records(fetched) => {
                    let mut drain = None;
                    match self.deliver(fetched, &sink, &shutdown, &mut drain).await {
                        Delivery::Done => {}
                        Delivery::Shutdown | Delivery::Closed => break,
                    }
                }
                // The next `next` commits what was accepted and rejoins.
                Next::Revoke(_) => {}
            }
        }
        if let Some(consumer) = self.consumer.as_mut() {
            consumer.leave().await;
        }
        Ok(())
    }
}
