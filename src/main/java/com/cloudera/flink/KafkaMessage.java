package com.cloudera.flink;

import org.apache.kafka.common.header.Headers;

public class KafkaMessage {
    String topic;
    int partition;
    long timestamp;
    byte[] key;
    byte[] value;
    Headers headers;

    KafkaMessage() {}

    KafkaMessage(String topic, int partition, long timestamp, byte[] key, byte[] value, Headers headers) {
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s, %s, [%s-byte key], [%s-byte value], %s)", KafkaMessage.class.getSimpleName(),
                topic, partition, timestamp,
                key == null ? "null" : String.format("%s-byte", key.length),
                value == null ? "null" : String.format("%s-byte", value.length),
                headers);
    }

}
