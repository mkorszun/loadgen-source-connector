Kafka Connect Load Generator connector
======================================

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) LoadGen connector allows generating desired load on the specific Kafka topic.

# Configuration

Connector has following configuration options:

| Name                         | Description                                      | Type      | Default Value         |
|------------------------------|--------------------------------------------------|-----------|-----------------------|
| `message`                    | Message used in load test                        | `string`  |                       |
| `message.count`              | Number of messages to publish in each cycle      | `int`     | 100                   |
| `message.delay`              | Delay in [s] between message generation cycles   | `long`    | 1                     |
| `kafka.topic`                | Kafka topic to write to                          | `strings` |                       |
| `mapping.providerId`         | Provider identifier                              | `strings` |                       |
| `content.format`             | Format of the content: `byte_xml` or `byte_json` | `string`  | `byte_json`           |

# Example configuration:

~~~
message={"a":"b"}
mapping.providerId=123abc456
kafka.topic=output-topic
~~~

# Output Data Format

~~~json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "string",
        "optional": false,
        "field": "providerId"
      },
      {
        "type": "string",
        "optional": true,
        "field": "rawContentFormat"
      },
      {
        "type": "bytes",
        "optional": false,
        "field": "rawContent"
      }
    ],
    "optional": false,
    "name": "com.meltwater.kafka.connect.loadgen.model.OutputPayload",
    "doc": "Kafka output message"
  },
  "payload": {
    "providerId": "providerId",
    "rawContentFormat": "byte_json",
    "rawContent": "eyJhIjoiYiJ9"
  }
}
~~~

where `payload.rawContent` contains `base64` encoded json content (binary).

# Development

## Requirements

* [Java 11](https://openjdk.java.net/projects/jdk/11/)
* [Gradle 6.3](https://docs.gradle.org/6.3/release-notes.html)
* [Docker](https://www.docker.com/)

## Test
~~~bash
$ docker-compose -f docker-compose-test.yml up -d
$ ./gradlew test
~~~

## Release
~~~bash
$ ./gradlew clean shadowJar
~~~
