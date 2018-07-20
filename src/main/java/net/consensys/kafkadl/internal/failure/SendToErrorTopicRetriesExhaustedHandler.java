package net.consensys.kafkadl.internal.failure;

import lombok.AllArgsConstructor;
import net.consensys.kafkadl.internal.forwarder.ErrorTopicForwarder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class SendToErrorTopicRetriesExhaustedHandler implements DeadLetterRetriesExhaustedHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SendToErrorTopicRetriesExhaustedHandler.class);

    private ErrorTopicForwarder errorTopicForwarder;

    @Override
    public void onFailure(ConsumerRecord record) {
        errorTopicForwarder.forward(record);
    }
}
