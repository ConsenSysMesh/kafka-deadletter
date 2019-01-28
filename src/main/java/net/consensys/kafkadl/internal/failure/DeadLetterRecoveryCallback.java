package net.consensys.kafkadl.internal.failure;

import lombok.AllArgsConstructor;
import net.consensys.kafkadl.internal.DeadLetterTopicNameConvention;
import net.consensys.kafkadl.internal.KafkaProperties;
import net.consensys.kafkadl.internal.util.JSON;
import net.consensys.kafkadl.message.ReflectionRetryableMessage;
import net.consensys.kafkadl.message.RetryableMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;

import java.util.Optional;

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

        getRetryableMessage(record).ifPresent(message -> {
            if (hasExhaustedRetries(message)) {
                LOG.error(String.format("Retries exhausted for message: %s", JSON.stringify(message)));
                retriesExhaustedHandler.onFailure(record);
            } else {

                final String key = record.key() != null ? record.key().toString() : null;

                LOG.error(String.format("Failed to process message: %s",
                        JSON.stringify(record.value())), context.getLastThrowable());
                LOG.info("Sending to dead letter topic");
                kafkaTemplate.send(deadLetterConvention.getDeadLetterTopicName(record.topic()), key, record.value());
            }
        });

        return null;
    }

    private Optional<RetryableMessage> getRetryableMessage(ConsumerRecord record) {
        final Object recordValue = record.value();

        if (recordValue instanceof RetryableMessage) {
            return Optional.of((RetryableMessage) recordValue);
        }

        if (ReflectionRetryableMessage.isSupported(recordValue)) {
            return Optional.of(new ReflectionRetryableMessage(recordValue));
        }

        return Optional.empty();
    }

    private boolean hasExhaustedRetries(RetryableMessage message) {
        return message.getRetries() >= getMaxRetries();
    }

    private int getMaxRetries() {
        return kafkaProperties.getDeadLetterTopicRetries();
    }
}
