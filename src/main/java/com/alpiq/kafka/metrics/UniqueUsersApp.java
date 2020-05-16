package com.alpiq.kafka.metrics;

import com.alpiq.kafka.metrics.model.KafkaConfiguration;
import com.alpiq.kafka.metrics.service.KafkaConfigurationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class UniqueUsersApp {
    private final static Logger logger = LoggerFactory.getLogger(UniqueUsersApp.class.getName());

    public static void main(String[] args) {
        try {
            // Read the Kafka configuration for the consumer
            KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService();
            KafkaConfiguration kafkaConfiguration = kafkaConfigurationService.getKafkaConfiguration();

            String applicationId = kafkaConfiguration.getApplicationId();
            String bootstrapServers = kafkaConfiguration.getBootstrapServers();
            String consumerGroupId = kafkaConfiguration.getConsumerGroupId();
            String consumerTopicName = kafkaConfiguration.getConsumerTopicName();
            String producerTopicName = kafkaConfiguration.getProducerTopicName();

            logger.info("Application Id="+applicationId);
            logger.info("BootstrapServers="+bootstrapServers);

            Properties properties = new Properties();
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // TODO not for production
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error "+e.getMessage()); // TODO
        }
    }
}
