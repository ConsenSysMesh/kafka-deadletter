package net.consensys.kafkadl.message;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;

@Data
@NoArgsConstructor
public class DummyDetails {
    String stringValue;
    BigInteger bigIntegerValue;
}
