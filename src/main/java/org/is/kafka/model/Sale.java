package org.is.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;
import lombok.Builder;

@Builder
public record Sale(
    UUID id,
    Sock sock,
    @JsonProperty("customer_name")
    String customerName,
    @JsonProperty("price_per_pair")
    Double pricePerPair,
    int quantity
) {
}
