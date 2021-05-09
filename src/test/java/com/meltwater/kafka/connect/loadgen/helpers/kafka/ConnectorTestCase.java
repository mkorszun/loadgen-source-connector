package com.meltwater.kafka.connect.loadgen.helpers.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class ConnectorTestCase {
  protected static final String OUTPUT_TOPIC = getOutputTopic();
  protected static final String BOOTSTRAP_SERVERS = getBootstrapServers();

  protected TestConnector testConnector;
  protected TestConsumer testConsumer;

  protected static Map<String, String> workerProps() {
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put("key.converter.schemas.enable", "true");
    workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put("value.converter.schemas.enable", "true");
    workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put("internal.key.converter.schemas.enable", "true");
    workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
    workerProps.put("internal.value.converter.schemas.enable", "true");
    workerProps.put("rest.port", "8086");
    workerProps.put("rest.host.name", "0.0.0.0");
    workerProps.put("offset.storage.file.filename", "build/" + UUID.randomUUID().toString());
    workerProps.put("offset.flush.interval.ms", "500");
    return workerProps;
  }

  private static String getBootstrapServers() {
    Optional<String> bootstrapServers = Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS"));
    return bootstrapServers.orElse("localhost:9092");
  }

  private static String getOutputTopic() {
    Optional<String> outputTopic = Optional.ofNullable(System.getenv("OUTPUT_TOPIC"));
    return outputTopic.orElse("test_topic");
  }

  @BeforeEach
  public void setup() {
    testConsumer = new TestConsumer(BOOTSTRAP_SERVERS, OUTPUT_TOPIC);
    testConnector = new TestConnector();
    testConnector.setWorkerProperties(workerProps());
  }

  @AfterEach
  public void cleanup() {
    testConsumer.unsubscribe();
    testConsumer.close();

    testConnector.stop();
    testConnector.deleteOffsetsFile();
  }
}
