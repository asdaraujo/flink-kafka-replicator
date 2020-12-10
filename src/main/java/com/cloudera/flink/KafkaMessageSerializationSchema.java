package com.cloudera.flink;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaMessageSerializationSchema implements KafkaSerializationSchema<KafkaMessage> {
    private boolean ignorePartition = false;

    KafkaMessageSerializationSchema(boolean ignorePartition) {
        this.ignorePartition = ignorePartition;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaMessage msg, @Nullable Long aLong) {
        return new ProducerRecord<>(
                msg.topic,
                ignorePartition ? null : msg.partition,
                msg.timestamp,
                msg.key,
                msg.value,
                msg.headers);
    }
}
