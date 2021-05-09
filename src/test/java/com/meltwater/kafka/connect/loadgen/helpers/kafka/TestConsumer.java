package com.meltwater.kafka.connect.loadgen.helpers.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static org.awaitility.Awaitility.await;

public class TestConsumer extends KafkaConsumer<String, String> {

  public TestConsumer(String bootStrapServers, String topic) {
    super(config(bootStrapServers));
    this.subscribe(Collections.singletonList(topic));
  }

  private static Properties config(String bootStrapServers) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return props;
  }

  public List<ConsumerRecord<String, String>> consume(int count) {
    return consume(count, 1);
  }

  public List<ConsumerRecord<String, String>> consume(int count, int maxWaitInMinutes) {
    final List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();

    await()
        .atMost(new org.awaitility.Duration(maxWaitInMinutes, TimeUnit.MINUTES))
        .until(
            () -> {
              this.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
              return consumerRecords.size() >= count;
            });

    return consumerRecords;
  }
}
