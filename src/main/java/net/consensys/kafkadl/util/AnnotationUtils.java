package net.consensys.kafkadl.util;

import net.consensys.kafkadl.annotation.DeadLetterMessage;

public class AnnotationUtils {

    public static boolean isDeadLetterMessageAnnotated(Object object) {
        return object.getClass().isAnnotationPresent(DeadLetterMessage.class);
    }
}
