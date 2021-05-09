package com.meltwater.kafka.connect.loadgen;

import com.meltwater.kafka.connect.loadgen.model.OutputPayload;
import com.meltwater.kafka.connect.loadgen.model.SchemaHelper;
import com.meltwater.kafka.connect.loadgen.model.SourceRecordBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadGenSourceTask extends SourceTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadGenSourceTask.class);
  private static final Map<String, ?> NO_OFFSET = Collections.emptyMap();
  private static final Map<String, ?> NO_PARTITION = Collections.emptyMap();
  private final CountDownLatch stopLatch = new CountDownLatch(1);

  private boolean shouldWait;
  private LoadGenSourceConfig sourceConfig;

  @Override
  public String version() {
    return LoadGenSourceConnector.CONNECTOR_VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    this.sourceConfig = new LoadGenSourceConfig(props);
  }

  @Override
  @SuppressWarnings("PMD")
  public List<SourceRecord> poll() throws InterruptedException {
    boolean shouldStop = false;
    if (shouldWait) {
      LOGGER.debug("Waiting for {} seconds", sourceConfig.getMessageDelay());
      shouldStop = stopLatch.await(sourceConfig.getMessageDelay(), TimeUnit.SECONDS);
    }
    if (!shouldStop) {
      shouldWait = true;
      LOGGER.debug("Sending {} messages", sourceConfig.getMessageCount());
      return Collections.nCopies(sourceConfig.getMessageCount(), record());
    } else {
      LOGGER.debug("Received signal to stop...");
      return null;
    }
  }

  private SourceRecord record() {
    OutputPayload payload =
        OutputPayload.builder()
            .providerId(sourceConfig.getProviderId())
            .rawContentFormat(sourceConfig.getContentFormat().name())
            .rawContent(sourceConfig.getMessage().getBytes())
            .build();

    return new SourceRecordBuilder()
        .withStruct(payload.toStruct())
        .withSourcePartition(NO_PARTITION)
        .withSourceOffset(NO_OFFSET)
        .withTopic(sourceConfig.getTopic())
        .withValueSchema(SchemaHelper.VALUE_SCHEMA)
        .build();
  }

  @Override
  public void stop() {
    stopLatch.countDown();
  }
}
