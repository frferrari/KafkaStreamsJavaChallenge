package com.alpiq.kafka.metrics.consumer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Instant;

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
        System.out.println("DeduplicateValueTransformer key="+key+" value="+value+" context.timestamp="+context.timestamp());
        if (valueStore.fetch(value, context.timestamp()) == null) {
            valueStore.put(value, value, context.timestamp());
            dumpWindowStore();
            return value;
        } else {
            dumpWindowStore();
            return null;
        }
    }

    @Override
    public void close() {
        System.out.println("deduplicateValueTransformer:: Closing *****");
        dumpWindowStore();
        // Note: The store should NOT be closed manually here via `valueStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }

    public void dumpWindowStore() {
        // 1468244340L - 1468244520L
        KeyValueIterator<Windowed<V>, V> it = valueStore.fetchAll(Instant.ofEpochSecond(1468244000L), Instant.ofEpochSecond(1468245000L));
        for (int idx=1; it.hasNext(); idx=idx+1) {
            KeyValue<Windowed<V>, V> item = it.next();
            System.out.println("("+idx+") Window="+item.key+" v="+item.value);
            // System.out.println("("+idx+") Window="+item.key.window().start()+"/"+item.key.window().end()+" v="+item.value);
        }
        it.close();
        System.out.println("-------------------------------------------------------------------------------------");
    }
}
