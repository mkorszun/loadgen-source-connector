package com.meltwater.kafka.connect.loadgen.model;

import lombok.experimental.UtilityClass;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

@UtilityClass
public final class SchemaHelper {
  public static final String PROVIDER_ID = "providerId";
  public static final String RAW_CONTENT = "rawContent";
  public static final String RAW_CONTENT_FORMAT = "rawContentFormat";
  public static final Schema VALUE_SCHEMA = valueSchema();

  private static Schema valueSchema() {
    return SchemaBuilder.struct()
        .name(OutputPayload.class.getName())
        .doc("Kafka output message")
        .field(PROVIDER_ID, Schema.STRING_SCHEMA)
        .field(RAW_CONTENT_FORMAT, Schema.OPTIONAL_STRING_SCHEMA)
        .field(RAW_CONTENT, Schema.BYTES_SCHEMA)
        .build();
  }
}
