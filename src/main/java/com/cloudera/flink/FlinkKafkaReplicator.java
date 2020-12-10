package com.cloudera.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FlinkKafkaReplicator {
    final private static Logger LOG = LoggerFactory.getLogger(FlinkKafkaReplicator.class);
    final private static int REQUEST_TIMEOUT_MS = 30_000;
    final private static short REPLICATION_FACTOR = 1;
    final private static int DEFAULT_CHECKPOINT_INTERVAL_MS = 1_000;
    final private static int DEFAULT_TRANSACTION_TIMEOUT_MS = 870_000;

    final private static String TOPICS = "topics";
    final private static String CONSUMER_PREFIX = "consumer.";
    final private static String PRODUCER_PREFIX = "producer.";
    private static enum OPTIONS {
        CHECKPOINT_INTERVAL_MS,
        CREATE_PARTITIONS,
        CREATE_TOPICS,
        EXACTLY_ONCE,
        LATEST_OFFSET,
        NO_PRESERVE_PARTITIONING,
        SYNC_PROPERTIES,
        TRANSACTION_TIMEOUT_MS;

        @Override
        public String toString() {
            return super.toString().toLowerCase().replace("_", "-");
        }
    }
    final private static String[] REQUIRED_PARAMS = {
            TOPICS,
            CONSUMER_PREFIX + "bootstrap.servers",
            CONSUMER_PREFIX + "group.id",
            PRODUCER_PREFIX + "bootstrap.servers",
    };

    public static void main(String[] args) throws Exception {
        LOG.info("Starting {}", FlinkKafkaReplicator.class.getSimpleName());

        List<String> argList = Arrays.stream(args).map(s -> s.replaceAll("(<<|>>)", "\"")).collect(Collectors.toList());
        args = argList.toArray(args);
        for (String opt : args) LOG.debug("Arg: {}", opt);
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!validate_params(params)) {
            print_syntax();
            throw new RuntimeException("Invalid or missing parameters");
        }

        String topicsPattern = params.getRequired(TOPICS);
        boolean createTopics = params.has(OPTIONS.CREATE_TOPICS.toString());
        boolean createPartitions = params.has(OPTIONS.CREATE_PARTITIONS.toString());
        boolean syncProperties = params.has(OPTIONS.SYNC_PROPERTIES.toString());
        boolean ignorePartitioning = params.has(OPTIONS.NO_PRESERVE_PARTITIONING.toString());
        boolean latestOffset = params.has(OPTIONS.LATEST_OFFSET.toString());
        boolean exactlyOnce = params.has(OPTIONS.EXACTLY_ONCE.toString());
        int checkpointIntervalMs = params.getInt(OPTIONS.CHECKPOINT_INTERVAL_MS.toString(), DEFAULT_CHECKPOINT_INTERVAL_MS);
        int transactionTimeoutMs = params.getInt(OPTIONS.TRANSACTION_TIMEOUT_MS.toString(), DEFAULT_TRANSACTION_TIMEOUT_MS);

        if (exactlyOnce && transactionTimeoutMs < checkpointIntervalMs) {
            throw new RuntimeException(
                    String.format("Transaction timeout (%d ms) must be greater than the checkpoint interval (%d ms)",
                            transactionTimeoutMs, checkpointIntervalMs));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10_000));
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(checkpointIntervalMs);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties consumerProps = getPropertiesWithPrefix(params.getProperties(), CONSUMER_PREFIX);
        Properties producerProps = getPropertiesWithPrefix(params.getProperties(), PRODUCER_PREFIX);
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);

        checkTopics(consumerProps, producerProps, createTopics, createPartitions, syncProperties, topicsPattern, ignorePartitioning);

        FlinkKafkaConsumer<KafkaMessage> consumer = new FlinkKafkaConsumer<KafkaMessage>(
                Pattern.compile(topicsPattern),
                new KafkaMessageDeserializationSchema(),
                consumerProps);
        if (latestOffset) {
            consumer.setStartFromLatest();
        } else {
            consumer.setStartFromEarliest();
        }

        FlinkKafkaProducer<KafkaMessage> producer = new FlinkKafkaProducer<KafkaMessage>(
                "unknown-topic",
                new KafkaMessageSerializationSchema(ignorePartitioning),
                producerProps,
                exactlyOnce ? FlinkKafkaProducer.Semantic.EXACTLY_ONCE : FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        DataStream<KafkaMessage> messageStream = env
                .addSource(consumer)
                .name("Read From Kafka")
                .uid("Read From Kafka");

        messageStream
                .addSink(producer)
                .name("Write To Kafka")
                .uid("Write To Kafka");

        env.execute("FlinkReadWriteKafka");
    }

    private static boolean validate_params(ParameterTool params) {
        boolean unknownOption = false;
        for (String opt : params.toMap().keySet().stream().sorted().collect(Collectors.toList())) {
            boolean unknown = false;
            if (!TOPICS.equals(opt) &&
                    Arrays.stream(OPTIONS.values()).map(String::valueOf).noneMatch(opt::equals) &&
                    !opt.startsWith(CONSUMER_PREFIX) &&
                    !opt.startsWith(PRODUCER_PREFIX))
                unknown = true;
            LOG.debug("Parameter: [{}]{} = [{}]", opt, unknown ? " (UNKNOWN ARGUMENT)" : "", params.get(opt, "<no argument>"));
            unknownOption |= unknown;
        }
        return !(unknownOption || !Arrays.stream(REQUIRED_PARAMS).allMatch(params::has));
    }

    private static void print_syntax() {
        System.out.printf("Usage: %s [options] \\\n", FlinkKafkaReplicator.class.getSimpleName());
        System.out.printf("         --%s <topic_pattern> \\\n", TOPICS);
        System.out.printf("         --%s.bootstrap.servers <kafka brokers> \\\n", CONSUMER_PREFIX);
        System.out.printf("         --%s.group.id <groupid> \\\n", CONSUMER_PREFIX);
        System.out.printf("         [--%s.<kafka_consumer_property> <value> ...] \\\n", CONSUMER_PREFIX);
        System.out.printf("         --%s.bootstrap.servers <kafka brokers> \\\n", PRODUCER_PREFIX);
        System.out.printf("         [--%s.<kafka_producer_property> <value> ...]", PRODUCER_PREFIX);
        System.out.println("\nValid options:");
        for (OPTIONS opt : OPTIONS.values()) {
            System.out.printf("         --%s\n", opt);
        }
    }

    static private Properties getPropertiesWithPrefix(Properties props, String pattern) {
        Properties prefixedProps = new Properties();
        String prefixPattern = "^" + pattern;
        for (String key : props.stringPropertyNames()) {
            if (key.matches(prefixPattern + ".*")) {
                prefixedProps.put(key.replaceFirst(prefixPattern, ""), props.getProperty(key));
            }
        }
        return prefixedProps;
    }

    static private Map<String, Topic> getTopics(Properties props, String topicsPattern) throws ExecutionException, InterruptedException {
        Pattern regex = Pattern.compile(topicsPattern);
        AdminClient admin = KafkaAdminClient.create(props);
        Set<String> names = admin.listTopics().names().get().stream().filter(s -> regex.matcher(s).matches()).collect(Collectors.toSet());
        Map<String, Topic> topics = admin.describeTopics(names, new DescribeTopicsOptions().timeoutMs(REQUEST_TIMEOUT_MS))
                .all().get().entrySet()
                .stream()
                .map(entry -> new Topic(entry.getKey(), entry.getValue().partitions().size(), new Properties()))
                .collect(Collectors.toMap(t -> t.name, t -> t));
        Set<ConfigResource> configResources = names.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).collect(Collectors.toSet());
        DescribeConfigsResult configsResult = admin.describeConfigs(configResources, new DescribeConfigsOptions().timeoutMs(REQUEST_TIMEOUT_MS));
        for(Map.Entry<ConfigResource, Config> entry : configsResult.all().get().entrySet()) {
            for (ConfigEntry config : entry.getValue().entries()) {
                topics.get(entry.getKey().name()).config.put(config.name(), config.value());
            }
        }
        return topics;
    }

    static private void checkTopics(Properties consumerProps, Properties producerProps,
                                    boolean createTopics, boolean createPartitions, boolean syncProperties,
                                    String topicsPattern, boolean ignorePartitioning)
            throws ExecutionException, InterruptedException {
        Map<String, Topic> consumerTopics = getTopics(consumerProps, topicsPattern);
        Map<String, Topic> producerTopics = getTopics(producerProps, topicsPattern);

        AdminClient admin = KafkaAdminClient.create(producerProps);

        boolean failed = false;
        for(Topic topic : consumerTopics.values()) {
            if (!producerTopics.containsKey(topic.name)) {
                if (!createTopics) {
                    LOG.error("Topic [{}] does not exist on target. You can create it manually or use the --{} option.",
                            topic.name, OPTIONS.CREATE_TOPICS);
                    failed = true;
                } else {
                    LOG.info("Creating topic [{}] on target.", topic.name);
                    admin.createTopics(
                            Collections.singleton(
                                    new NewTopic(topic.name, topic.partitions, REPLICATION_FACTOR)
                                            .configs(topic.getConfigMap())),
                            new CreateTopicsOptions().timeoutMs(REQUEST_TIMEOUT_MS));
                }
            } else {
                Topic targetTopic = producerTopics.get(topic.name);
                if (topic.partitions > targetTopic.partitions) {
                    if (!createPartitions) {
                        String msg = String.format("Target topic [%s] has less partitions (%s) than source (%s). ",
                                topic.name, targetTopic.partitions, topic.partitions);
                        if (ignorePartitioning) {
                            LOG.warn(msg + "Will continue and redistribute messages across the existing partitions.");
                        } else {
                            LOG.error(msg + "You can add partitions manually or use the --{} option. " +
                                            "If you want to keep the number of target partitions you must specify the --{} option to continue.",
                                    OPTIONS.CREATE_PARTITIONS, OPTIONS.NO_PRESERVE_PARTITIONING);
                            failed = true;
                        }
                    } else {
                        LOG.info("Target topic [{}] has less partitions ({}) than source ({}). Adding partitions to target to match source.",
                                topic.name, targetTopic.partitions, topic.partitions);
                        admin.createPartitions(
                                new HashMap<String, NewPartitions>() {{ put(topic.name, NewPartitions.increaseTo(topic.partitions)); }},
                                new CreatePartitionsOptions().timeoutMs(REQUEST_TIMEOUT_MS));
                    }
                } else if (topic.partitions < targetTopic.partitions) {
                    LOG.warn("Target topic [{}] has more partitions ({}) than source ({}).", topic.name, targetTopic.partitions, topic.partitions);
                }

                for(String prop : topic.config.stringPropertyNames()) {
                    String sourceValue = topic.config.getProperty(prop, null);
                    String targetValue = targetTopic.config.getProperty(prop, null);
                    if (sourceValue != null && targetValue != null && !sourceValue.equals(targetValue) ||
                            sourceValue != null && targetValue == null ||
                            sourceValue == null && targetValue != null) {
                        if (!syncProperties) {
                            LOG.warn("Property [{}] of topic [{}] has a different value on the target ({}) than source ({}).",
                                    prop, topic.name,
                                    sourceValue == null ? "<null>" : sourceValue,
                                    targetValue == null ? "<null>" : targetValue);
                        } else {
                            LOG.info("Property [{}] of topic [{}] has a different value on the target ({}) than source ({}). Updating the value on the target.",
                                    prop, topic.name,
                                    sourceValue == null ? "<null>" : sourceValue,
                                    targetValue == null ? "<null>" : targetValue);
                            admin.alterConfigs(new HashMap<ConfigResource, Config>() {{
                                put(new ConfigResource(ConfigResource.Type.TOPIC, topic.name),
                                        new Config(Collections.singleton(new ConfigEntry(prop, sourceValue))));
                            }});
                        }
                    }
                }
            }
        }
        admin.close(Duration.ofMillis(REQUEST_TIMEOUT_MS));

        if (failed)
            throw new RuntimeException("Review and address the errors above before running the job again.");

    }
}
