package com.cloudera.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMessageDeserializationSchema implements KafkaDeserializationSchema<KafkaMessage> {
    @Override
    public boolean isEndOfStream(KafkaMessage kafkaMessage) {
        return false;
    }

    @Override
    public KafkaMessage deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return new KafkaMessage(
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.timestamp(),
                consumerRecord.key(),
                consumerRecord.value(),
                consumerRecord.headers());
    }

    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        return TypeInformation.of(KafkaMessage.class);
    }
}
