package net.consensys.kafkadl.internal.integration;

import net.consensys.kafkadl.internal.DeadLetterTopicNameConvention;
import net.consensys.kafkadl.internal.KafkaProperties;
import net.consensys.kafkadl.internal.util.JSON;
import net.consensys.kafkadl.message.RetryableMessage;
import net.consensys.kafkadl.message.RetryableMessageStringWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ErrorHandler;

public class DeadLetterTopicConsumer extends BatchContinueOnFailureConsumer<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterTopicConsumer.class);

    private KafkaTemplate<String, String> kafkaTemplate;
    private DeadLetterTopicNameConvention dltConvention;

    public DeadLetterTopicConsumer(ProducerFactory<String,String> producerFactory,
                                   ErrorHandler errorHandler,
                                   DeadLetterTopicNameConvention dltConvention,
                                   KafkaProperties kafkaProperties) {
        super(kafkaProperties, errorHandler);
        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
        this.dltConvention = dltConvention;
    }


    @Override
    void onMessage(ConsumerRecord<String, String> record) {
        final RetryableMessage retryableMessage = new RetryableMessageStringWrapper(record.value());
        LOG.info(String.format("Processing message from Dead Letter Topic: %s", record.value()));

        //Increment retries
        retryableMessage.setRetries(retryableMessage.getRetries() + 1);

        LOG.info("Retry attempt number: " + retryableMessage.getRetries());
        final String topicName = dltConvention.getOriginalTopicFromDeadLetterTopicName(record.topic());

        kafkaTemplate.send(topicName, record.key(), retryableMessage.toString());
    }
}
