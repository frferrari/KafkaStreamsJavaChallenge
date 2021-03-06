package com.alpiq.kafka.metrics;

import com.alpiq.kafka.metrics.consumer.DeduplicateValueTransformer;
import com.alpiq.kafka.metrics.consumer.HashSetStringSerde;
import com.alpiq.kafka.metrics.consumer.LogFrameTimestampExtractor;
import com.alpiq.kafka.metrics.model.KafkaConfiguration;
import com.alpiq.kafka.metrics.service.KafkaConfigurationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
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

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class UniqueUsersApp {
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm";
    public static final String uidStoreName = "uid-store";
    public static final String countStoreName = "count-store";
    private final static Logger logger = LoggerFactory.getLogger(UniqueUsersApp.class.getName());

    public static void main(String[] args) {
        try {
            // Read the Kafka configuration for the consumer
            KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService();
            KafkaConfiguration kafkaConfiguration = kafkaConfigurationService.getKafkaConfiguration();
            Properties streamsConfiguration = getStreamsConfiguration(kafkaConfiguration);

            // Build the kafka stream
            final StreamsBuilder builder = new StreamsBuilder();
            createUniqueUsersCountStream(builder, kafkaConfiguration);
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
        streamsConfiguration.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.String().getClass());
        // To clean up window stores https://sachabarbs.wordpress.com/category/kafka/
        // streamsConfiguration.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 10000); // milliseconds
        try {
            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("tumbling-windows").toAbsolutePath().toString());
        } catch (IOException e) {
            // If we can't have a temporary directory, then we keep the default value
            logger.warn("Unable to define a temp directory for STATE_DIR_CONFIG: " + e.getMessage());
        }

        return streamsConfiguration;
    }

    static void createUniqueUsersCountStream(final StreamsBuilder builder, KafkaConfiguration kafkaConfiguration) {
        final KStream<String, String> logFrames = builder.stream(kafkaConfiguration.getConsumerTopicName());

        // We use a tumbling window, 1 minute window size
        // See https://kafka-tutorials.confluent.io/create-tumbling-windows/kstreams.html
        final Duration windowSize = Duration.ofMinutes(1);
        TimeWindows tw = TimeWindows.of(windowSize);

        // Deduplicating events
        // See https://blog.softwaremill.com/de-de-de-de-duplicating-events-with-kafka-streams-ed10cfc59fbe
        final StoreBuilder<WindowStore<String, String>> deduplicationValueStoreBuilder =
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(uidStoreName,
                                windowSize,
                                windowSize,
                                false
                        ),
                        Serdes.String(),
                        Serdes.String());
        builder.addStateStore(deduplicationValueStoreBuilder);

        KStream<String, String> uniqueUsers = logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupByKey()
                .windowedBy(tw)
                .aggregate(() -> "", (tsMinute, uid, agg) -> uid)
                .transformValues(() -> new DeduplicateValueTransformer<>(uidStoreName), uidStoreName)
                // .suppress(Suppressed.untilWindowCloses(unbounded())) // Could not make it work
                .toStream()
                // Duplicates are marked as null by the DeduplicateValueTransformer, we must remove this events
                // It is mandatory to have this filterNot after the toStream and not before it
                .filterNot((k, v) -> v == null)
                .peek(UniqueUsersApp::peekLoggerWindowed)
                .map((Windowed<String> k, String v) -> new KeyValue<>(k.key(), v));

        uniqueUsers
                .groupByKey()
                .count()
                .mapValues(Object::toString)
                .toStream()
                // .peek((k, v) -> logger.info("k=" + k + " count=" + v))
                .to(kafkaConfiguration.getProducerTopicName());
    }

    /*
     * Below are experiments that will be removed from the final code base
     */
    static void createUniqueUsersStreamWithWindowStore(final StreamsBuilder builder, KafkaConfiguration kafkaConfiguration) {
        final KStream<String, String> logFrames = builder.stream(kafkaConfiguration.getConsumerTopicName());

        // We use a tumbling window, 1 minute window size
        // See https://kafka-tutorials.confluent.io/create-tumbling-windows/kstreams.html
        final Duration windowSize = Duration.ofMinutes(1);
        TimeWindows tw = TimeWindows.of(windowSize);

        Materialized<String, String, WindowStore<Bytes, byte[]>> materialized =
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("unique-users-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String());

        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupByKey()
                .windowedBy(tw)
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .peek((k, v) -> logger.info("k=" + k.key() + " windowStart=" + k.window().start() + " windowEnd=" + k.window().end() + " v=" + v));
    }

    static void createUniqueUsersStreamWithMaterializedHashSet(final StreamsBuilder builder, KafkaConfiguration kafkaConfiguration) {
        final KStream<String, String> logFrames = builder.stream(kafkaConfiguration.getConsumerTopicName());

        Serde<HashSet<String>> hashSetSerde = new HashSetStringSerde();

        // We use a tumbling window, 1 minute window size
        // See https://kafka-tutorials.confluent.io/create-tumbling-windows/kstreams.html
        final Duration windowSize = Duration.ofMinutes(1);
        TimeWindows tw = TimeWindows.of(windowSize);

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

    /**
     * Utility function that calls peek to log information regarding the current record
     * This function is used to process windowed keys
     *
     * @param wk    The windowed record key
     * @param value The record value
     */
    static public <K, V> void peekLoggerWindowed(Windowed<K> wk, V value) {
        logger.info("k=" + wk.key() + " window().start/end=" +wk.window().start() + "/" + wk.window().end() + " window().startTime/endTime=" + wk.window().startTime() + "/" + wk.window().endTime() + " v=" + value);
    }

    /**
     * Utility function that calls peek to log information regarding the current record
     * This function is used to process non windowed keys
     *
     * @param k     The record key
     * @param value The record value
     */
    static public <K, V> void peekLogger(K k, V value) {
        logger.info("k=" + k + " v=" + value);
    }
}
