package com.alpiq.kafka.metrics;

import com.alpiq.kafka.metrics.model.KafkaConfiguration;
import com.alpiq.kafka.metrics.service.KafkaConfigurationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

public class UniqueUsersApp {
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm";
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

    static Properties getStreamsConfiguration(KafkaConfiguration kafkaConfiguration) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfiguration.getApplicationId());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // TODO not for production
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // TODO streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        return streamsConfiguration;
    }

    static void createUniqueUsersStream(final StreamsBuilder builder, KafkaConfiguration kafkaConfiguration) {
        final KStream<String, String> logFrames = builder.stream(kafkaConfiguration.getConsumerTopicName());
        String sep = "~";

        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupBy((tsMinute, uid) -> tsMinute + sep + uid)
                .reduce((tsMinuteUid, uid) -> "")
                .toStream()
                // .peek((k, v) -> logger.info("k="+k+ " v="+v))
                .map((k, v) -> new KeyValue<>(k.split(sep)[0], k.split(sep)[1]))
                // .peek((k, v) -> logger.info("k="+k+ " v="+v))
                .groupByKey()
                .count()
                .toStream()
                .peek((k, v) -> logger.info("k="+k+ " v="+v))
                .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.Long()));
        /*
        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty()) // reject any record whose timestamp could not be converted to tsMinute
                .groupByKey()
                .aggregate(() -> 0L,
                        (tsMinute, uid, agg) -> { logger.info("k="+tsMinute+" uid="+uid+" agg="+agg); return agg + 1; },
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("unique-users-count-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()))
                .toStream()
                .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.Long()));
         */

        /*
        logFrames
                .mapValues(UniqueUsersApp::processRecord)
                .filterNot((tsMinute, uid) -> uid.isEmpty())
                .groupByKey()
                .aggregate(() -> new HashSet<String>(),
                        (tsMinute, uid, agg) -> {
                            agg.add(uid);
                            return agg;
                        },
                        Materialized.<String, HashSet<String>, KeyValueStore<Bytes, byte[]>>as("unique-users-count"))
//        Materialized.<String, byte[], KeyValueStore<Bytes, byte[]>>as("unique-users-count")
//                .withKeySerde(Serdes.String()).withValueSerde(Serdes.ByteArray()))
                //.mapValues(HashSet::size)
                .toStream();
                // .to(kafkaConfiguration.getProducerTopicName(), Produced.with(Serdes.String(), Serdes.Integer()));
         */
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
