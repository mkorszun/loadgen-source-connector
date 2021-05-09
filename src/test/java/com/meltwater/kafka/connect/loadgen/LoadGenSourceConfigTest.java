package com.meltwater.kafka.connect.loadgen;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@TestMethodOrder(MethodOrderer.MethodName.class)
class LoadGenSourceConfigTest {

  @Test
  public void testConfig() {
    LoadGenSourceConfig config = new LoadGenSourceConfig(originals());

    assertThat(config.getMessage()).isEqualTo(originals().get(LoadGenSourceConfig.MESSAGE));
    assertThat(config.getProviderId()).isEqualTo(originals().get(LoadGenSourceConfig.PROVIDER_ID));
  }

  @Test
  public void testConfigDefaultValues() {
    LoadGenSourceConfig config = new LoadGenSourceConfig(originals());

    assertThat(config.getMessageCount()).isEqualTo(100);
    assertThat(config.getMessageDelay()).isEqualTo(1);
    assertThat(config.getContentFormat()).isEqualTo(LoadGenContentFormat.byte_json);
  }

  private Map<String, String> originals() {
    Map<String, String> originals = new HashMap<>();
    originals.put(LoadGenSourceConfig.MESSAGE, "{\"a\":\"b\"}");
    originals.put(LoadGenSourceConfig.PROVIDER_ID, "providerId");
    originals.put(LoadGenSourceConfig.TOPIC, "output_topic");
    return originals;
  }
}
