package com.meltwater.kafka.connect.loadgen;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum LoadGenContentFormat {
  byte_xml,
  byte_json;

  public static List<String> names() {
    return Arrays.stream(LoadGenContentFormat.values())
        .map(Enum::name)
        .collect(Collectors.toList());
  }
}
