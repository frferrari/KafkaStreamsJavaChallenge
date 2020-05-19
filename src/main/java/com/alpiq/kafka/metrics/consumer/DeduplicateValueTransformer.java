package com.alpiq.kafka.metrics.consumer;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Discards duplicate uids
 * See: https://kafka-tutorials.confluent.io/finding-distinct-events/kstreams.html
 * @param <K>
 * @param <V>
 * @param <E>
 */
public class DeduplicateValueTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V> {

    private ProcessorContext context;
    private final String valueStoreName;

    public DeduplicateValueTransformer(String valueStoreName) {
        this.valueStoreName = valueStoreName;
    }

    private WindowStore<V, V> valueStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        valueStore = (WindowStore<V, V>) context.getStateStore(valueStoreName);
    }

    @Override
    public V transform(final K key, final V value) {
        if (valueStore.fetch(value, context.timestamp()) == null) {
            valueStore.put(value, value, context.timestamp());
            return value;
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        // Note: The store should NOT be closed manually here via `valueStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }
}
