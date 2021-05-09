package com.meltwater.kafka.connect.loadgen;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@TestMethodOrder(MethodOrderer.MethodName.class)
class LoadGenSourceConnectorTest {

  @Test
  public void testTaskConfigs() {
    LoadGenSourceConnector connector = new LoadGenSourceConnector();
    Map<String, String> originals = originals();

    connector.start(originals);
    List<Map<String, String>> configs = connector.taskConfigs(1);

    assertThat(configs).asList().hasSize(1);
    assertThat(configs.get(0)).isEqualTo(originals);
  }

  @Test
  public void testTaskConfigsForMultipleTasks() {
    LoadGenSourceConnector connector = new LoadGenSourceConnector();
    Map<String, String> originals = originals();

    connector.start(originals);
    List<Map<String, String>> configs = connector.taskConfigs(3);

    assertThat(configs).asList().hasSize(3);
    assertThat(configs.get(0)).isEqualTo(originals);
    assertThat(configs.get(1)).isEqualTo(originals);
    assertThat(configs.get(2)).isEqualTo(originals);
  }

  @Test
  public void failsToStartConnectorWhenRequiredConfigurationIsMissing() {
    LoadGenSourceConnector connector = new LoadGenSourceConnector();

    assertThatThrownBy(() -> connector.start(Collections.emptyMap()))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration");
  }

  @Test
  public void failsToStartConnectorWhenUnknownFormatSpecified() {
    LoadGenSourceConnector connector = new LoadGenSourceConnector();
    Map<String, String> originals = originals();
    originals.put(LoadGenSourceConfig.CONTENT_FORMAT, "image");

    assertThatThrownBy(() -> connector.start(originals))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid value image for configuration content.format: should be one of: byte_xml,byte_json");
  }

  private Map<String, String> originals() {
    Map<String, String> originals = new HashMap<>();
    originals.put(LoadGenSourceConfig.MESSAGE, "{\"a\":\"b\"}");
    originals.put(LoadGenSourceConfig.PROVIDER_ID, "providerId");
    originals.put(LoadGenSourceConfig.TOPIC, "output_topic");
    return originals;
  }
}
