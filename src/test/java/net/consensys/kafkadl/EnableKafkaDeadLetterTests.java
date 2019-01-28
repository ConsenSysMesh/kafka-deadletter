package net.consensys.kafkadl;

import junit.framework.TestCase;

import net.consensys.kafkadl.internal.DeadLetterTopicNameConvention;
import net.consensys.kafkadl.internal.util.JSON;
import net.consensys.kafkadl.message.DummyDetails;
import net.consensys.kafkadl.message.DummyMessage;
import net.consensys.kafkadl.message.DummyMessageNoInterface;
import net.consensys.kafkadl.message.TestMessage;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = {TestApplication.class, TestConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(locations="classpath:application-test.properties")
public class EnableKafkaDeadLetterTests {

    private static final String KAFKA_LISTENER_CONTAINER_ID = "org.springframework.kafka.KafkaListenerEndpointContainer#0";

    private static final int DEFAULT_INTERNAL_RETRIES = 3;

    private static final int DEFAULT_DEADLETTER_RETRIES = 3;

    private static final String STRING_VALUE = "0x8f981487bc415153a1a56cb5e7b17ec11e36bf2aeb6576bbb8fbf2e51ac9785c";

    private static final BigInteger BIG_INT_VALUE = BigInteger.TEN;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, "testTopic");

    @LocalServerPort
    private int port = 12345;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    public KafkaListenerEndpointRegistry registry;

    @Autowired
    private DeadLetterTopicNameConvention dltNameConvention;

    private List<TestMessage> receivedMessagesOnMainTopic;

    private List<TestMessage> receivedMessagesOnErrorTopic;

    private ErrorCriteria errorCriteria;

    @Before
    public void init() throws Exception {
        receivedMessagesOnMainTopic = new ArrayList<>();
        receivedMessagesOnErrorTopic = new ArrayList<>();

        final MessageListenerContainer container = registry.getListenerContainer(KAFKA_LISTENER_CONTAINER_ID);

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    @Test
    public void testMessageProcessedCorrectly() {
        configureErrorCriteria(false, null);

        final DummyMessage message = sendMessageAndWait(1, 0);

        assertEquals(1, receivedMessagesOnMainTopic.size());
        assertEquals(message.getId(), receivedMessagesOnMainTopic.get(0).getId());
    }

    @Test
    public void testMessageFailureRecoverOnFirstDeadLetterRetry() {
        doFailureWithDeadLetterRecovery(1);
    }

    @Test
    public void testMessageFailureRecoverOnSecondDeadLetterRetry() {
        doFailureWithDeadLetterRecovery(2);
    }

    @Test
    public void testMessageFailureRecoverOnThirdDeadLetterRetry() {
        doFailureWithDeadLetterRecovery(3);
    }

    @Test
    public void testMessageFailureWithNoRecovery() {
        configureErrorCriteria(true, null);
        final int expectedMessageCount =
                DEFAULT_INTERNAL_RETRIES * DEFAULT_DEADLETTER_RETRIES + DEFAULT_INTERNAL_RETRIES;
        final DummyMessage message = sendMessageAndWait(expectedMessageCount, 1);

        assertEquals(expectedMessageCount, receivedMessagesOnMainTopic.size());
        assertEquals(1, receivedMessagesOnErrorTopic.size());
        assertEquals(message.getId(), receivedMessagesOnErrorTopic.get(0).getId());
    }

    @Test
    public void testNonInterfaceImplementingMessageFailureWithNoRecovery() {
        configureErrorCriteria(true, null);
        final int expectedMessageCount =
                DEFAULT_INTERNAL_RETRIES * DEFAULT_DEADLETTER_RETRIES + DEFAULT_INTERNAL_RETRIES;

        final DummyDetails details = new DummyDetails();
        details.setStringValue(STRING_VALUE);
        details.setBigIntegerValue(BIG_INT_VALUE);

        final DummyMessageNoInterface message = new DummyMessageNoInterface();
        message.setDetails(details);

        sendMessage(message);
        waitForMessages(expectedMessageCount, 1);

        assertEquals(expectedMessageCount, receivedMessagesOnMainTopic.size());
        assertEquals(1, receivedMessagesOnErrorTopic.size());
        assertEquals(message.getDetails(), receivedMessagesOnErrorTopic.get(0).getDetails());
    }

    @Test
    public void testDltNameConventionDLTSuffixContainsServiceId() {
        assertTrue(dltNameConvention.getDeadLetterTopicSuffix().contains("TestApplication"));
    }

    @Test
    public void testDltNameConventionErrorSuffixContainsServiceId() {
        assertTrue(dltNameConvention.getErrorTopicSuffix().contains("TestApplication"));
    }

    private void doFailureWithDeadLetterRecovery(int retryNumberForRecovery) {
        configureErrorCriteria(true, retryNumberForRecovery);

        final DummyMessage message = sendMessage();

        assertCorrectMessagesReceived(message.getId(), retryNumberForRecovery);
    }

    private DummyMessage sendMessage() {
        final DummyMessage message = createDummyMessage();

        sendMessage(message);

        return message;
    }

    private void sendMessage(Object message) {
        kafkaTemplate.send("testTopic", message);
    }

    private DummyMessage sendMessageAndWait(int expectedMessagesOnMainTopic,
                                            int expectedMessagesOnErrorTopic) {
        final DummyMessage message = sendMessage();
        waitForMessages(expectedMessagesOnMainTopic, expectedMessagesOnErrorTopic);

        return message;
    }

    @KafkaListener(topics = "testTopic", groupId = "testGroup")
    public void consumer(TestMessage message) {
        System.out.println(String.format("Message received: %s", JSON.stringify(message)));
        receivedMessagesOnMainTopic.add(message);

        if (errorCriteria.shouldError
                && (errorCriteria.recoverOnRetryNumber == null
                || !errorCriteria.recoverOnRetryNumber.equals(message.getRetries()))) {
            throw new IllegalStateException("Forced failure!");
        }
    }

    @KafkaListener(topics = "#{deadLetterTopicNameConvention.getErrorTopicName('testTopic')}",
            groupId = "testGroup")
    public void errorConsumer(TestMessage message) {
        System.out.println(String.format("Message received on error topic: %s", JSON.stringify(message)));
        receivedMessagesOnErrorTopic.add(message);
    }

    private DummyMessage createDummyMessage() {
        final DummyDetails details = new DummyDetails();
        details.setStringValue(STRING_VALUE);
        details.setBigIntegerValue(BIG_INT_VALUE);

        final DummyMessage message = new DummyMessage();
        message.setDetails(details);

        return message;
    }

    private void configureErrorCriteria(boolean shouldError, Integer recoverOnRetryNumber) {
        errorCriteria = new ErrorCriteria(shouldError, recoverOnRetryNumber);
    }

    private void waitForMessages(int expectedMessagesOnMainTopic,
                                 int expectedMessagesOnErrorTopic) {
        //Wait for an initial 2 seconds (this is usually enough time and is needed
        //in order to catch failures when no messages are expected on error topic but one arrives)
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Wait for another 8 seconds maximum if messages have not yet arrived
        final long startTime = System.currentTimeMillis();
        while(true) {
            if (receivedMessagesOnMainTopic.size() == expectedMessagesOnMainTopic
                    && receivedMessagesOnErrorTopic.size() == expectedMessagesOnErrorTopic) {
                break;
            }

            if (System.currentTimeMillis() > startTime + 8000) {
                final StringBuilder builder = new StringBuilder("Failed to receive all expected messages");
                builder.append("\n");
                builder.append("Expected main topic messages: " + expectedMessagesOnMainTopic);
                builder.append(", received: " + receivedMessagesOnMainTopic.size());
                builder.append("\n");
                builder.append("Expected error topic messages: " + expectedMessagesOnErrorTopic);
                builder.append(", received: " + receivedMessagesOnErrorTopic.size());

                TestCase.fail(builder.toString());
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private int getExpectedMessageCountOnRetryRecovery(int retryNumberForRecovery) {
        return DEFAULT_INTERNAL_RETRIES * retryNumberForRecovery + 1;
    }

    private void assertCorrectMessagesReceived(String id, int retryNumberForRecovery) {
        waitForMessages(getExpectedMessageCountOnRetryRecovery(retryNumberForRecovery), 0);

        assertEquals(getExpectedMessageCountOnRetryRecovery(retryNumberForRecovery), receivedMessagesOnMainTopic.size());

        for(int i = 0; i < receivedMessagesOnMainTopic.size(); i++) {
            final TestMessage message = receivedMessagesOnMainTopic.get(i);
            assertEquals(id, message.getId());

            final Integer expectedRetryNumber = i / (DEFAULT_INTERNAL_RETRIES);

            assertEquals(expectedRetryNumber, message.getRetries());
        }
    }

    private class ErrorCriteria {
        boolean shouldError;
        Integer recoverOnRetryNumber;

        private ErrorCriteria(boolean shouldError, Integer recoverOnRetryNumber) {
            this.shouldError = shouldError;
            this.recoverOnRetryNumber = recoverOnRetryNumber;
        }
    }
}
