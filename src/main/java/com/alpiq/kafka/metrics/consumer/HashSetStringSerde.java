package com.alpiq.kafka.metrics.consumer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.*;

/**
 * A HashSet<String> serializer/deserializer
 */
public class HashSetStringSerde extends Serdes.WrapperSerde<HashSet<String>> {
    public HashSetStringSerde() {
        super(new Serializer<HashSet<String>>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {
            }

            @Override
            public byte[] serialize(String topic, HashSet<String> elements) {
                Iterator<String> it = elements.iterator();
                StringBuilder str = new StringBuilder();
                while (it.hasNext()) {
                    str.append(it.next()).append("|");
                }
                return str.toString().getBytes();
            }

            @Override
            public void close() {
            }
        }, new Deserializer<HashSet<String>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public HashSet<String> deserialize(String topic, byte[] data) {
                List<String> list = Arrays.asList(new String(data).split("\\|"));
                return new HashSet<String>(list);
            }

            @Override
            public void close() {
            }
        });
    }
}
