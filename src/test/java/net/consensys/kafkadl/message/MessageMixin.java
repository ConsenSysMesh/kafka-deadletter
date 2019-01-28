package net.consensys.kafkadl.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DummyMessage.class, name = DummyMessage.TYPE),
        @JsonSubTypes.Type(value = DummyMessageNoInterface.class, name = DummyMessageNoInterface.TYPE),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface MessageMixin {
}
