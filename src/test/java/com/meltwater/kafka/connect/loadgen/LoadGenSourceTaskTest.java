package com.meltwater.kafka.connect.loadgen;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@TestMethodOrder(MethodOrderer.MethodName.class)
class LoadGenSourceTaskTest {
  @Test
  public void pollReturnsGeneratedMessages() throws InterruptedException {
    LoadGenSourceTask task = new LoadGenSourceTask();
    task.start(originals());

    assertThat(task.poll()).asList().hasSize(100);
  }

  @Test
  public void pollSetsAllOutputFields() throws InterruptedException {
    LoadGenSourceTask task = new LoadGenSourceTask();
    task.start(originals());

    SourceRecord record = task.poll().get(0);
    Struct payload = (Struct) record.value();

    assertThat(payload.get("providerId")).isEqualTo("providerId");
    assertThat(payload.get("rawContentFormat")).isEqualTo("byte_json");
    assertThat(payload.getBytes("rawContent")).asString().isEqualTo("{\"a\":\"b\"}");
  }

  private Map<String, String> originals() {
    Map<String, String> originals = new HashMap<>();
    originals.put(LoadGenSourceConfig.MESSAGE, "{\"a\":\"b\"}");
    originals.put(LoadGenSourceConfig.PROVIDER_ID, "providerId");
    originals.put(LoadGenSourceConfig.TOPIC, "output_topic");
    return originals;
  }
}
