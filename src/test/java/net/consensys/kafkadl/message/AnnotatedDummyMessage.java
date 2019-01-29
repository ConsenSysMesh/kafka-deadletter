package net.consensys.kafkadl.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.consensys.kafkadl.annotation.DeadLetterMessage;

@DeadLetterMessage
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotatedDummyMessage extends DummyMessage {

    public static final String TYPE = "ANNOTATED-DUMMY";

    @Override
    public String getType() {
        return TYPE;
    }
}
