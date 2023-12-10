package org.is.kafka.model;

import lombok.Builder;

@Builder
public record Result(
    String id,
    String requirement,
    String result
) {
}
