package com.meltwater.kafka.connect.loadgen.config;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class EnumValidator implements ConfigDef.Validator {
  private final List<String> values;

  public EnumValidator(String... values) {
    this(Arrays.asList(values));
  }

  public EnumValidator(List<String> values) {
    this.values = values;
  }

  @Override
  public void ensureValid(String name, Object value) {
    String val = String.valueOf(value);
    if (value != null && !values.contains(val)) {
      String msg = String.format("should be one of: %s", String.join(",", values));
      throw new ConfigException(name, value, msg);
    }
  }
}
