package com.alpiq.kafka.metrics;

import com.alpiq.kafka.metrics.consumer.HashSetStringSerde;
import com.alpiq.kafka.metrics.consumer.LogFrameTimestampExtractor;
import com.alpiq.kafka.metrics.model.KafkaConfiguration;
import com.alpiq.kafka.metrics.service.KafkaConfigurationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

public class UniqueUsersApp {
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm";
    public static final String logFrameStoreName = "log-frames-store";
    private final static Logger logger = LoggerFactory.getLogger(UniqueUsersApp.class.getName());

    public static void main(String[] args) {
        try {
            // Read the Kafka configuration for the consumer
            KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService();
            KafkaConfiguration kafkaConfiguration = kafkaConfigurationService.getKafkaConfiguration();
            Properties streamsConfiguration = getStreamsConfiguration(kafkaConfiguration);

            // Build the kafka stream
            final StreamsBuilder builder = new StreamsBuilder();
            createUniqueUsersStream(builder, kafkaConfiguration);
            final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

            streams.cleanUp();
            streams.start();

            // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error " + e.getMessage()); // TODO refine
        }
    }

    /**
     * Produces the streams configuration
     *
     * @param kafkaConfiguration The configuration as defined in the config.properties file
     * @return A Properties object containing the configuration for our kafka topology
     */
    static Properties getStreamsConfiguration(KafkaConfiguration kafkaConfiguration) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfiguration.getApplicationId());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // TODO earliest is not for production
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LogFrameTimestampExtractor.class.getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try {
            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("tumbling-windows").toAbsolutePath().toString());
        } catch (IOException e) {
            // If we can't have a temporary directory, then we keep the default value
            logger.warn("Unable to define a temp directory for STATE_DIR_CONFIG: " + e.getMessage());
        }

        return streamsConfiguration;
    }

    /**
     * Produces the kafka topology for our unique users metrics
     *
     * @param builder            A Streams builders
     * @param kafkaConfiguration An object containing the configuration of consumer/produced topic names, etc ..
     */
    static void createUniqueUsersStream(final StreamsBuilder builder, KafkaConfiguration kafkaConfiguration) {
        final KStream<String, String> logFrames = builder.stream(kafkaConfiguration.getConsumerTopicName());

        Serde<HashSet<String>> hashSetSerde = new HashSetStringSerde();

        // We use a tumbling window, 1 minute window size
        // See https://kafka-tutorials.confluent.io/create-tumbling-windows/kstreams.html
        final Duration windowSize = Duration.ofMinutes(1);
        TimeWindows tw = TimeWindows.of(windowSize).advanceBy(windowSize);

        final StoreBuilder<WindowStore<String, String>> deduplicationStoreBuilder =
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(logFrameStoreName,
                                windowSize,
                                windowSize,
                                false
                        ),
                        Serdes.String(),
                        Serdes.String());
        builder.addStateStore(deduplicationStoreBuilder);

        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupByKey()
                .windowedBy(tw)
                .aggregate(() -> "",
                        (k, v, a) -> v)
                .toStream()
                .map((Windowed<String> k, String v) -> new KeyValue<>(k.key(), v))
                .transformValues(() -> new DeduplicationTransformer<>(), logFrameStoreName)
                .filter((k, v) -> v != null)
                .groupByKey()
                .count()
                .toStream()
                .peek((k, v) -> logger.info("k=" + k + " v=" + v))
                .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.Long()));

        /*
        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupByKey()
                .windowedBy(tw)
                .count()
                .toStream()
                .map((Windowed<String> k, Long v) -> new KeyValue<>(k.key(), v))
                .peek((k, v) -> logger.info("k=" + k + " v=" + v))
                .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.Long()));

         */

        /*
        Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized =
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("unique-users-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long());
        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupBy((k, v) -> k+"~"+v)
                .windowedBy(tw)
                .aggregate(() -> 0L,
                        (k, v, a) -> 1L,
                        materialized)
                .toStream()
                .map((Windowed<String> k, Long v) -> new KeyValue<>(k.key().split("~")[0], v))
                .peek((k, v) -> logger.info("k=" + k + " v=" + v))
                /*
                .groupByKey()
                .count()
                .toStream()
                .peek((k, v) -> logger.info("k=" + k + " v=" + v))
                .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.Long()));
        */

        /*
        Aggregator<String, String, HashSet<String>> uidAggregator = (tsMinute, uid, uids) -> {
            // System.out.println("tsMinute="+tsMinute+" uid="+uid+" uids.size="+uids.size());
            uids.add(uid);
            return uids;
        };

        Materialized<String, HashSet<String>, WindowStore<Bytes, byte[]>> materialized =
                Materialized.<String, HashSet<String>, WindowStore<Bytes, byte[]>>as("unique-users-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(hashSetSerde);

        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupByKey()
                .windowedBy(tw)
                .aggregate(() -> new HashSet<String>(),
                        uidAggregator,
                        materialized)
                // .suppress(Suppressed.untilWindowCloses(unbounded()))
                .mapValues(v -> Integer.toString(v.size()))
                .toStream()
                .map((Windowed<String> k, String v) -> new KeyValue<>(k.key(), v))
                .peek((k, v) -> logger.info("k=" + k + " v=" + v))
                .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.String()));
        */
    }

    /**
     * Discards duplicate uids
     * See: https://kafka-tutorials.confluent.io/finding-distinct-events/kstreams.html
     * @param <K>
     * @param <V>
     * @param <E>
     */
    public static class DeduplicationTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V> {

        private ProcessorContext context;

        /**
         * Key: uid
         * Value: dummy value
         */
        private WindowStore<V, V> uidStore; // TODO purge the window stores

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            this.context = context;
            uidStore = (WindowStore<V, V>) context.getStateStore(logFrameStoreName);
        }

        @Override
        public V transform(final K key, final V value) {
            if (uidStore.fetch(value, context.timestamp()) == null) {
                // logger.info("context.timestamp=" + context.timestamp() + " key=" + key + " value=" + value + " added");
                uidStore.put(value, value, context.timestamp());
                return value;
            } else {
                // logger.info("context.timestamp=" + context.timestamp() + " key=" + key + " value=" + value + " already exists, dropped");
                return null;
            }
        }

        @Override
        public void close() {
            // Note: The store should NOT be closed manually here via `uidStore.close()`!
            // The Kafka Streams API will automatically close stores when necessary.
        }
    }

    /**
     * Extracts and returns the uid node from a json string value given as an argument
     *
     * @param record A json string containing the uid node to extract
     * @return The value of the uid node or "" when not found
     */
    private static String processRecord(String record) {
        JSONParser parser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(record);
            return jsonObject.get("uid").toString();
        } catch (Exception e) {
            logger.error("Could not extract the uid field from the json payload, log frame rejected: " + record);
            return "";
        }
    }
}
