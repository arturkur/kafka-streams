package org.is.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Random;
import java.util.UUID;
import lombok.Builder;

@Builder
public record Sock(
    UUID id,
    Double price,
    SockType type,
    @JsonProperty("supplier_id")
    UUID supplierId
) {

  public enum SockType {
    INVISIBLE,
    LOW_CUT,
    OVER_THE_CALF,
    ;

    private static final Random RANDOM = new Random();

    public static SockType random() {
      var socks = values();
      return socks[RANDOM.nextInt(socks.length)];
    }
  }
}
