package com.meltwater.kafka.connect.loadgen.helpers.kafka;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderProvider;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NoArgsConstructor
public class TestConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestConnector.class);

  private Thread connectThread;
  private CountDownLatch stopLatch;

  @Setter private Map<String, String> connectorProperties;
  @Setter private Map<String, String> workerProperties;

  public void start() {
    stopLatch = new CountDownLatch(1);

    connectThread =
        new Thread(
            () -> {
              Map<String, String> workerProps = workerProperties;

              Plugins plugins = new Plugins(workerProps);
              plugins.compareAndSwapWithDelegatingLoader();
              StandaloneConfig config = new StandaloneConfig(workerProps);

              String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);

              RestServer rest = new RestServer(config);
              HerderProvider provider = new HerderProvider();
              rest.start(provider, plugins);

              URI advertisedUrl = rest.advertisedUrl();
              String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

              Worker worker =
                  new Worker(workerId, Time.SYSTEM, plugins, config, new FileOffsetBackingStore());

              Herder herder = new StandaloneHerder(worker, kafkaClusterId);
              final Connect connect = new Connect(herder, rest);

              LOGGER.info("Kafka Connect standalone worker has been initialized");

              try {
                connect.start();
                provider.setHerder(herder);
                Map<String, String> connectorProps = connectorProperties;
                FutureCallback<Herder.Created<ConnectorInfo>> cb =
                    new FutureCallback<>(
                        (error, info) -> {
                          if (error != null) {
                            LOGGER.error("Failed to create job", error);
                          } else {
                            LOGGER.info("Created connector {}", info.result().name());
                          }
                        });
                herder.putConnectorConfig(
                    connectorProps.get(ConnectorConfig.NAME_CONFIG), connectorProps, false, cb);
                cb.get();
              } catch (Throwable t) {
                LOGGER.error("Stopping after connector error", t);
                connect.stop();
                connect.awaitStop();
              }
              try {
                stopLatch.await();
              } catch (InterruptedException e) {
                LOGGER.info("Connect thread has been interrupted, stopping.");
                connect.stop();
                connect.awaitStop();
              }
              connect.stop();
              connect.awaitStop();
            });

    connectThread.start();
  }

  public void stop() {
    stopLatch.countDown();
    while (connectThread.isAlive()) {
      LOGGER.info("Waiting for the kafka connect to stop");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void deleteOffsetsFile() {
    new File(workerProperties.get("offset.storage.file.filename")).delete();
  }
}
