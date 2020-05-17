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
public class DeduplicationTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V> {

    private ProcessorContext context;
    private final String uidStoreName;

    public DeduplicationTransformer(String uidStoreName) {
        this.uidStoreName = uidStoreName;
    }

    /**
     * Key: uid
     * Value: dummy value
     */
    private WindowStore<V, V> uidStore; // TODO purge the window stores

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        uidStore = (WindowStore<V, V>) context.getStateStore(uidStoreName);
    }

    @Override
    public V transform(final K key, final V value) {
        if (uidStore.fetch(value, context.timestamp()) == null) {
            // System.out.println("context.timestamp=" + context.timestamp() + " key=" + key + " value=" + value + " added");
            uidStore.put(value, value, context.timestamp());
            return value;
        } else {
            // System.out.println("context.timestamp=" + context.timestamp() + " key=" + key + " value=" + value + " already exists, dropped");
            return null;
        }
    }

    @Override
    public void close() {
        // Note: The store should NOT be closed manually here via `uidStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }
}
