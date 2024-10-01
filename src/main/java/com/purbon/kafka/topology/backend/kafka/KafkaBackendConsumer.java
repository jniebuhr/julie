package com.purbon.kafka.topology.backend.kafka;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.BackendState;
import com.purbon.kafka.topology.backend.KafkaBackend;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class KafkaBackendConsumer {
  private static final Logger LOGGER = LogManager.getLogger(KafkaBackendConsumer.class);
  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private final Configuration config;
  private KafkaConsumer<String, BackendState> consumer;
  private TopicPartition assignedTopicPartition;

  private final AtomicBoolean running;

  public KafkaBackendConsumer(Configuration config) {
    this.config = config;
    this.running = new AtomicBoolean(false);
  }

  public void configure() {
    Properties consumerProperties = config.asProperties();
    consumerProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
    var serde = new JsonDeserializer<>(BackendState.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, serde.getClass());

    consumerProperties.put(GROUP_ID_CONFIG, config.getKafkaBackendConsumerGroupId());
    consumer = new KafkaConsumer<>(consumerProperties);

    List<PartitionInfo> partitions = consumer.partitionsFor(config.getJulieKafkaConfigTopic(), TIMEOUT);
    if (partitions.size() > 1) {
      LOGGER.warn("The configured state topic has more than one partition. This can potentially cause problems.");
    }

    assignedTopicPartition = new TopicPartition(config.getJulieKafkaConfigTopic(), 0);
    var topicPartitions = Collections.singletonList(assignedTopicPartition);
    consumer.assign(topicPartitions);
    consumer.seekToBeginning(topicPartitions);
  }

  public void retrieve(KafkaBackend callback) {
    int times = 0;
    while (running.get()) {
      ConsumerRecords<String, BackendState> records = consumer.poll(TIMEOUT);
      callback.complete();
      for (ConsumerRecord<String, BackendState> record : records) {
        callback.apply(record);
      }
      if (isTopicRead()) {
        LOGGER.info("Finished reading state after {} tries.", times);
        callback.initialLoadFinish();
      }
      times++;
    }
  }

  private boolean isTopicRead() {
    try {
      long position = consumer.position(assignedTopicPartition, TIMEOUT);
      long endOffset = consumer.endOffsets(Collections.singletonList(assignedTopicPartition), TIMEOUT)
              .get(assignedTopicPartition);
      return position >= endOffset;
    } catch (TimeoutException e) {
      LOGGER.warn("Timeout while reading offsets", e);
      return false;
    }
  }

  public void stop() {
    running.set(false);
    consumer.wakeup();
  }

  public void start() {
    running.set(true);
  }

  public Map<String, List<PartitionInfo>> listTopics() {
    return consumer.listTopics();
  }
}
