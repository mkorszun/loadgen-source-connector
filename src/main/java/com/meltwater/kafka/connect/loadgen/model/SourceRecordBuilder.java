package com.meltwater.kafka.connect.loadgen.model;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class SourceRecordBuilder {
  private Struct struct;
  private Map<String, ?> sourcePartition;
  private Map<String, ?> sourceOffset;
  private String topic;
  private Schema valueSchema;

  public SourceRecordBuilder withStruct(Struct struct) {
    this.struct = struct;
    return this;
  }

  public SourceRecordBuilder withSourcePartition(Map<String, ?> partition) {
    this.sourcePartition = partition;
    return this;
  }

  public SourceRecordBuilder withTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public SourceRecordBuilder withSourceOffset(Map<String, ?> offset) {
    this.sourceOffset = offset;
    return this;
  }

  public SourceRecordBuilder withValueSchema(Schema schema) {
    this.valueSchema = schema;
    return this;
  }

  public SourceRecord build() {
    return new SourceRecord(sourcePartition, sourceOffset, topic, null, valueSchema, struct);
  }
}
