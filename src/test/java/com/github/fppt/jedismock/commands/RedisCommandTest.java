package com.github.fppt.jedismock.commands;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class RedisCommandTest {
    @Test
    void equalsHashcode() {
        EqualsVerifier.forClass(RedisCommand.class)
                .withNonnullFields("parameters")
                .verify();
    }
}