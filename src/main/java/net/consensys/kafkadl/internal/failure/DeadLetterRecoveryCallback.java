package net.consensys.kafkadl.internal.failure;

import lombok.AllArgsConstructor;
import net.consensys.kafkadl.internal.DeadLetterTopicNameConvention;
import net.consensys.kafkadl.internal.KafkaProperties;
import net.consensys.kafkadl.internal.util.JSON;
import net.consensys.kafkadl.message.RetryableMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;

@AllArgsConstructor
public class DeadLetterRecoveryCallback implements RecoveryCallback {

    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterRecoveryCallback.class);

    private KafkaTemplate kafkaTemplate;
    private DeadLetterTopicNameConvention deadLetterConvention;
    private DeadLetterRetriesExhaustedHandler retriesExhaustedHandler;
    private KafkaProperties kafkaProperties;

    @Override
    public Object recover(RetryContext context) throws Exception {
        final ConsumerRecord record = (ConsumerRecord) context.getAttribute("record");

        if (record.value() instanceof RetryableMessage) {
            final RetryableMessage message = (RetryableMessage) record.value();

            if (hasExhaustedRetries(message)) {
                LOG.error(String.format("Retries exhausted for message: %s", JSON.stringify(message)));
                retriesExhaustedHandler.onFailure(record);
                return null;
            }

            final String key = record.key() != null ? record.key().toString() : null;

            LOG.error(String.format("Failed to process message: %s",
                    JSON.stringify(message)), context.getLastThrowable());
            LOG.info("Sending to dead letter topic");
            kafkaTemplate.send(deadLetterConvention.getDeadLetterTopicName(record.topic()), key, message);
        }

        return null;
    }

    private boolean hasExhaustedRetries(RetryableMessage message) {
        return message.getRetries() >= getMaxRetries();
    }

    private int getMaxRetries() {
        return kafkaProperties.getDeadLetterTopicRetries();
    }
}
