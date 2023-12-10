package org.is.kafka.model;

import java.util.UUID;

public record Supplier(
    UUID id,
    String name
) {
}
