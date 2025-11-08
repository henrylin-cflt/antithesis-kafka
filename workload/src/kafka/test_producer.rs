use crate::{
    config::WorkloadConfig,
    decisions::AddMessageKey,
    domain::{sequence::Sequence, GlobalState, ProducerConfig, TestTopic},
    rng,
};
use anyhow::{Context, Result};
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
    ClientConfig,
};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tracing::{error, info, warn};

pub struct TestProducer {
    pub id: String,
    inner_producer: FutureProducer,
    global_state: Arc<RwLock<GlobalState>>,
    producer_config: ProducerConfig,
}

impl TestProducer {
    pub fn new(
        id: &str,
        config: &WorkloadConfig,
        global_state: Arc<RwLock<GlobalState>>,
    ) -> Result<TestProducer> {
        let mut producer_config = ProducerConfig::new();
        producer_config
            .set("client.id", id)
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "0")
            .set("request.required.acks", "all")
            .set("enable.idempotence", "true");

        // Only set transactional.id if transactions are enabled
        if config.enable_transactions {
            let transactional_id = format!("{}-tx", id);
            producer_config.set("transactional.id", &transactional_id);
        }

        if config.enable_debug {
            producer_config.set("debug", "protocol");
        }
        let inner_producer: FutureProducer = ClientConfig::from(&producer_config)
            .create()
            .context("failed to create kafka producer")?;
        
        // Initialize transactions only if enabled
        if config.enable_transactions {
            inner_producer
                .init_transactions(Timeout::Never)
                .context("failed to initialize transactions")?;
        }
        
        let producer = TestProducer {
            id: String::from(id),
            inner_producer,
            global_state,
            producer_config,
        };
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "producer_created",
            producer_id = producer.id,
            enable_transactions = config.enable_transactions,
            config = serde_json::to_string(&producer.producer_config)
                .unwrap()
                .as_str()
        );
        Ok(producer)
    }
    pub async fn produce(self, config: WorkloadConfig, topics: Vec<TestTopic>) {
        tokio::time::sleep(Duration::from_millis(
            config.producer_startup_delay_ms.get_random_value(),
        ))
        .await;
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "producer_started",
            producer_id = self.id,
            config = serde_json::to_string(&self.producer_config)
                .unwrap()
                .as_str()
        );
        let unique_sequence_count = config.producer_unique_sequence_count.get_random_value();
        for _ in 0..unique_sequence_count {
            let sequence_length = config.producer_sequence_length.get_random_value();
            let sequence_name = uuid::Uuid::new_v4().to_string();

            let topic_name = topics[rng::u64_in(0, topics.len() as u64 - 1) as usize]
                .name
                .clone();
            let sequence_name = sequence_name.clone();
            let key = match antithesis_sdk::random::random_choice(&[
                AddMessageKey::Yes,
                AddMessageKey::No,
            ])
            .unwrap()
            {
                AddMessageKey::Yes => Some(uuid::Uuid::new_v4().to_string()),
                AddMessageKey::No => None,
            };
            self.produce_sequence(&config, &topic_name, key, &sequence_name, sequence_length)
                .await;
        }
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "producer_stopped",
            producer_id = self.id,
        );
    }

    async fn produce_sequence(
        &self,
        config: &WorkloadConfig,
        topic: &str,
        key: Option<String>,
        sequence_name: &str,
        sequence_length: u64,
    ) {
        if config.enable_transactions {
            self.produce_sequence_with_transaction(config, topic, key, sequence_name, sequence_length).await;
        } else {
            self.produce_sequence_without_transaction(config, topic, key, sequence_name, sequence_length).await;
        }
    }

    async fn produce_sequence_with_transaction(
        &self,
        config: &WorkloadConfig,
        topic: &str,
        key: Option<String>,
        sequence_name: &str,
        sequence_length: u64,
    ) {
        // Begin transaction for this sequence
        match self.inner_producer.begin_transaction() {
            Ok(_) => {
                info!(
                    timestamp = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "transaction_begun",
                    producer_id = self.id,
                    topic_name = topic,
                    sequence_name = sequence_name,
                );
            }
            Err(err) => {
                error!(
                    timestamp = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "transaction_begin_failed",
                    producer_id = self.id,
                    topic_name = topic,
                    sequence_name = sequence_name,
                    error = format!("{:#}", err).as_str(),
                );
                return;
            }
        }

        let sequence = Sequence::new(sequence_name, sequence_length);
        let mut messages_sent = 0;
        // Store messages that were successfully sent in the transaction
        // We'll only log them as "write_succeeded" after commit
        let mut pending_messages: Vec<(i32, i64, String, Option<String>)> = Vec::new();
        
        for next_value in sequence {
            let mut version = 0;
            loop {
                version += 1;
                let payload = format!("{}:{}|{}", self.id, version, next_value);
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    config.producer_sequence_delay_ms.get_random_value(),
                ))
                .await;
                let record = {
                    let record = FutureRecord::<str, String>::to(topic).payload(&payload);
                    if let Some(ref key) = key {
                        record.key(key)
                    } else {
                        record
                    }
                };
                match self
                    .inner_producer
                    .send(record, Timeout::Never)
                    .await
                {
                    Ok((partition, offset)) => {
                        messages_sent += 1;
                        // Store message info but don't log as "write_succeeded" yet
                        // We'll log it only after transaction commits
                        pending_messages.push((partition, offset, payload.clone(), key.clone()));
                        break;
                    }
                    Err((err, _)) => {
                        error!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "message_write_failed",
                            producer_id = self.id,
                            topic_name = topic,
                            message_key = key,
                            message_payload = payload,
                            error = format!("{:#}", err).as_str(),
                            code = format!("{:?}", err.rdkafka_error_code().unwrap())
                        );
                    }
                }
            }
        }

        // Decide whether to commit or abort transaction (30% chance to abort)
        let should_abort = rng::u64_in(1, 100) <= 30;
        
        if should_abort {
            match self.inner_producer.abort_transaction(Timeout::Never) {
                Ok(_) => {
                    warn!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "transaction_aborted",
                        producer_id = self.id,
                        topic_name = topic,
                        sequence_name = sequence_name,
                        messages_sent = messages_sent,
                    );
                    // Don't log message_write_succeeded for aborted transactions
                    // These messages were never actually written to Kafka
                }
                Err(err) => {
                    error!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "transaction_abort_failed",
                        producer_id = self.id,
                        topic_name = topic,
                        sequence_name = sequence_name,
                        error = format!("{:#}", err).as_str(),
                    );
                }
            }
        } else {
            match self.inner_producer.commit_transaction(Timeout::Never) {
                Ok(_) => {
                    // Now that transaction is committed, log all messages as successfully written
                    for (partition, offset, payload, msg_key) in pending_messages {
                        let mut global_state = self.global_state.write().unwrap();
                        global_state
                            .topic_partition_offsets
                            .entry(topic.to_string())
                            .or_default()
                            .insert(partition, offset);
                        info!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "message_write_succeeded",
                            producer_id = self.id,
                            topic_name = topic,
                            topic_partition = partition,
                            topic_partition_offset = offset,
                            message_key = msg_key,
                            message_payload = payload,
                        );
                    }
                    info!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "transaction_committed",
                        producer_id = self.id,
                        topic_name = topic,
                        sequence_name = sequence_name,
                        messages_sent = messages_sent,
                    );
                }
                Err(err) => {
                    error!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "transaction_commit_failed",
                        producer_id = self.id,
                        topic_name = topic,
                        sequence_name = sequence_name,
                        error = format!("{:#}", err).as_str(),
                    );
                    // If commit fails, don't log messages as written
                    // The transaction was aborted by Kafka
                }
            }
        }
    }

    async fn produce_sequence_without_transaction(
        &self,
        config: &WorkloadConfig,
        topic: &str,
        key: Option<String>,
        sequence_name: &str,
        sequence_length: u64,
    ) {
        let sequence = Sequence::new(sequence_name, sequence_length);
        for next_value in sequence {
            let mut version = 0;
            loop {
                version += 1;
                let payload = format!("{}:{}|{}", self.id, version, next_value);
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    config.producer_sequence_delay_ms.get_random_value(),
                ))
                .await;
                let record = {
                    let record = FutureRecord::<str, String>::to(topic).payload(&payload);
                    if let Some(ref key) = key {
                        record.key(key)
                    } else {
                        record
                    }
                };
                match self
                    .inner_producer
                    .send(record, Timeout::Never)
                    .await
                {
                    Ok((partition, offset)) => {
                        let mut global_state = self.global_state.write().unwrap();
                        global_state
                            .topic_partition_offsets
                            .entry(topic.to_string())
                            .or_default()
                            .insert(partition, offset);
                        info!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "message_write_succeeded",
                            producer_id = self.id,
                            topic_name = topic,
                            topic_partition = partition,
                            topic_partition_offset = offset,
                            message_key = key,
                            message_payload = payload
                        );
                        break;
                    }
                    Err((err, _)) => {
                        error!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "message_write_failed",
                            producer_id = self.id,
                            topic_name = topic,
                            message_key = key,
                            message_payload = payload,
                            error = format!("{:#}", err).as_str(),
                            code = format!("{:?}", err.rdkafka_error_code().unwrap())
                        );
                    }
                }
            }
        }
    }
}
