package com.alpiq.kafka.metrics.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class LogFrameTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try {
            return Long.parseLong((String) record.key()) * 1000;
        } catch(NumberFormatException e) {
            return 0;
        }
    }
}
