package net.consensys.kafkadl.message;

public interface TestMessage {
    String getId();
    String getType();
    DummyDetails getDetails();
    Integer getRetries();
}
