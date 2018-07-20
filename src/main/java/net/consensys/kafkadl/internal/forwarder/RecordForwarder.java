package net.consensys.kafkadl.internal.forwarder;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordForwarder<K, V> {
    void forward(ConsumerRecord<K, V> record);
}
