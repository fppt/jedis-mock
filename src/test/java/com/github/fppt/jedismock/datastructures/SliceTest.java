package com.github.fppt.jedismock.datastructures;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class SliceTest {
    @Test
    void equalsHashCode() {
        EqualsVerifier.forClass(Slice.class)
                .withNonnullFields("storedData")
                .verify();
    }
}