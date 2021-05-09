package com.meltwater.kafka.connect.loadgen;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class LoadGenSourceConnector extends SourceConnector {
  public static final String CONNECTOR_VERSION = "0.0.1";
  private LoadGenSourceConfig sourceConfig;

  @Override
  public void start(Map<String, String> props) {
    this.sourceConfig = new LoadGenSourceConfig(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return LoadGenSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return Collections.nCopies(maxTasks, sourceConfig.originalsStrings());
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return LoadGenSourceConfig.config();
  }

  @Override
  public String version() {
    return CONNECTOR_VERSION;
  }
}
