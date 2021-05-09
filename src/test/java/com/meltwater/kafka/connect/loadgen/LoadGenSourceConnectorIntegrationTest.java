package com.meltwater.kafka.connect.loadgen;

import com.meltwater.kafka.connect.loadgen.helpers.kafka.ConnectorTestCase;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LoadGenSourceConnectorIntegrationTest extends ConnectorTestCase {
  @Test
  public void singleTaskProduces100Messages() {
    testConnector.setConnectorProperties(connectorProps(1, 100, 1));
    testConnector.start();

    List<ConsumerRecord<String, String>> consumerRecords = testConsumer.consume(100, 1);
    assertThat(consumerRecords).asList().hasSize(100);
  }

  @Test
  public void singleTaskProducesMessagesEverySecond() {
    testConnector.setConnectorProperties(connectorProps(1, 100, 1));
    testConnector.start();

    List<ConsumerRecord<String, String>> consumerRecords = testConsumer.consume(500, 1);
    assertThat(consumerRecords).asList().hasSize(500);
  }

  @Test
  public void threeTaskProduces300Messages() {
    testConnector.setConnectorProperties(connectorProps(1, 100, 3));
    testConnector.start();

    List<ConsumerRecord<String, String>> consumerRecords = testConsumer.consume(300, 1);
    assertThat(consumerRecords).asList().hasSize(300);
  }

  private Map<String, String> connectorProps(long freq, int count, int maxTasks) {
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("name", "test");
    workerProps.put("tasks.max", Integer.toString(maxTasks));
    workerProps.put("connector.class", LoadGenSourceConnector.class.getName());

    workerProps.put(LoadGenSourceConfig.MESSAGE, "{\"a\":\"b\"}");
    workerProps.put(LoadGenSourceConfig.MESSAGE_DELAY, String.valueOf(freq));
    workerProps.put(LoadGenSourceConfig.MESSAGE_COUNT, String.valueOf(count));
    workerProps.put(LoadGenSourceConfig.PROVIDER_ID, "providerId");
    workerProps.put(LoadGenSourceConfig.TOPIC, OUTPUT_TOPIC);
    return workerProps;
  }
}
