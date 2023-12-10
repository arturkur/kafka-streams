package org.is.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;
import lombok.Builder;

@Builder
public record Purchase(
    UUID id,
    @JsonProperty("sock_id")
    UUID sockId,
    int quantity,
    Sock sock
) {
}
