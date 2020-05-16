package com.alpiq.kafka.metrics.service;

import com.alpiq.kafka.metrics.model.KafkaConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfigurationService {
    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    InputStream inputStream;

    public KafkaConfiguration getKafkaConfiguration() throws IOException {
        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            String applicationId = prop.getProperty("applicationId");
            String bootstrapServers = prop.getProperty("bootstrapServers");
            String consumerGroupId = prop.getProperty("consumerGroupId");
            String consumerTopicName = prop.getProperty("consumerTopicName");
            String producerTopicName = prop.getProperty("producerTopicName");

            kafkaConfiguration.setApplicationId(applicationId);
            kafkaConfiguration.setBootstrapServers(bootstrapServers);
            kafkaConfiguration.setConsumerGroupId(consumerGroupId);
            kafkaConfiguration.setConsumerTopicName(consumerTopicName);
            kafkaConfiguration.setProducerTopicName(producerTopicName);

            inputStream.close();
        } catch (Exception e) {
            System.out.println("KafkaConfiguration exception: " + e);
        }

        return kafkaConfiguration;
    }
}
