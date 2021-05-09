package com.meltwater.kafka.connect.loadgen.model;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.connect.data.Struct;

@Data
@Builder
public class OutputPayload {
  private final String providerId;
  private final String rawContentFormat;
  private final byte[] rawContent;

  public Struct toStruct() {
    Struct envelope = new Struct(SchemaHelper.VALUE_SCHEMA);
    envelope.put(SchemaHelper.PROVIDER_ID, providerId);
    envelope.put(SchemaHelper.RAW_CONTENT_FORMAT, rawContentFormat);
    envelope.put(SchemaHelper.RAW_CONTENT, getRawContent());
    return envelope;
  }
}
