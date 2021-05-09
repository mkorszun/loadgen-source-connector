package com.meltwater.kafka.connect.loadgen;

import com.meltwater.kafka.connect.loadgen.config.EnumValidator;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.Type;

@Getter
@Setter
public class LoadGenSourceConfig extends AbstractConfig {
  public static final String MESSAGE = "message";
  public static final String MESSAGE_DOC = "Message used in load test";

  public static final String MESSAGE_COUNT = "message.count";
  public static final String MESSAGE_COUNT_DOC = "Number of messages to publish in each cycle";

  public static final String MESSAGE_DELAY = "message.delay";
  public static final String MESSAGE_DELAY_DOC = "Delay in seconds between message generation cycles";

  public static final String PROVIDER_ID = "mapping.providerId";
  public static final String PROVIDER_ID_DOC = "Provider identifier";

  public static final String CONTENT_FORMAT = "content.format";
  public static final String CONTENT_FORMAT_DOC = "Format of the content: byte_xml or byte_json";

  public static final String TOPIC = "kafka.topic";
  public static final String TOPIC_DOC = "Kafka topic to write to";

  private static final EnumValidator FORMAT_VALIDATOR =
      new EnumValidator(LoadGenContentFormat.names());

  private String message;
  private int messageCount;
  private long messageDelay;

  private String providerId;
  private LoadGenContentFormat contentFormat;

  private String topic;

  public LoadGenSourceConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.message = getString(MESSAGE);
    this.messageCount = getInt(MESSAGE_COUNT);
    this.messageDelay = getLong(MESSAGE_DELAY);
    this.providerId = getString(PROVIDER_ID);
    this.topic = getString(TOPIC);
    this.contentFormat = LoadGenContentFormat.valueOf(getString(CONTENT_FORMAT));
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(MESSAGE, Type.STRING, Importance.HIGH, MESSAGE_DOC)
        .define(MESSAGE_COUNT, Type.INT, 100, Importance.LOW, MESSAGE_COUNT_DOC)
        .define(MESSAGE_DELAY, Type.LONG, 1, Importance.LOW, MESSAGE_DELAY_DOC)
        .define(PROVIDER_ID, Type.STRING, Importance.HIGH, PROVIDER_ID_DOC)
        .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
        .define(
            CONTENT_FORMAT,
            Type.STRING,
            "byte_json",
            FORMAT_VALIDATOR,
            Importance.LOW,
            CONTENT_FORMAT_DOC);
  }
}
