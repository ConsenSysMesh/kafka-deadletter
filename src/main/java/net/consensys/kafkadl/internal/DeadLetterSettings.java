package net.consensys.kafkadl.internal;

import lombok.Data;

import java.util.Set;

@Data
public class DeadLetterSettings {

    private Set<String> deadLetterEnabledTopics;
    private String serviceId;
    private Set<String> deadLetterEnabledContainerFactoryBeans;
}
