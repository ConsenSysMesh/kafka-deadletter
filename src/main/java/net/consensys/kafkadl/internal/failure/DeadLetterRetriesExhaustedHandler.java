package net.consensys.kafkadl.internal.failure;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface DeadLetterRetriesExhaustedHandler {
    void onFailure(ConsumerRecord record);
}
