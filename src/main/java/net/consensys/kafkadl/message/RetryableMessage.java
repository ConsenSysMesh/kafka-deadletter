package net.consensys.kafkadl.message;

public interface RetryableMessage {

    Integer getRetries();

    void setRetries(Integer numRetries);
}
