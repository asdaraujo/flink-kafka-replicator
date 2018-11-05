package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class FlinkReadWriteKafka {
    public static void main(String[] args) throws Exception {
        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: FlinkReadWriteKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer011<>(
                        params.getRequired("read-topic"),
                        new SimpleStringSchema(),
                        params.getProperties()));
         // Print Kafka messages to stdout - will be visible in logs
        messageStream.print();

        // If you want to perform some transformations before writing the data
        // back to Kafka, do it here!

        // Write payload back to Kafka topic
        messageStream.addSink(new FlinkKafkaProducer011<>(
                    params.getRequired("write-topic"),
                    new SimpleStringSchema(),
                    params.getProperties())).name("Write To Kafka");

        env.execute("FlinkReadWriteKafka");
    }
}
