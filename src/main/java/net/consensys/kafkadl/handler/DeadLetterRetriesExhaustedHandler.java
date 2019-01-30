package net.consensys.kafkadl.handler;

import org.springframework.retry.RetryContext;

public interface DeadLetterRetriesExhaustedHandler {
    void onFailure(RetryContext retryContext);
}
