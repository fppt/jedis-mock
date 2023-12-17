package com.github.fppt.jedismock.datastructures;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static com.github.fppt.jedismock.datastructures.Slice.create;
import static com.github.fppt.jedismock.datastructures.ZSetEntry.MAX_SCORE;
import static com.github.fppt.jedismock.datastructures.ZSetEntry.MAX_VALUE;
import static com.github.fppt.jedismock.datastructures.ZSetEntry.MIN_SCORE;
import static com.github.fppt.jedismock.datastructures.ZSetEntry.MIN_VALUE;

import static org.assertj.core.api.Assertions.assertThat;

class RMZSetTest {
    @Test
    void compareScore() {
        assertThat(new ZSetEntry(1, create("a"))
                .compareTo(new ZSetEntry(2, create("a")))).isLessThan(0);
    }

    @Test
    void compareLex() {
        assertThat(new ZSetEntry(1, create("a"))
                .compareTo(new ZSetEntry(1, create("b")))).isLessThan(0);
    }

    @Test
    void compareEquals() {
        assertThat(new ZSetEntry(1, create("a"))
                .compareTo(new ZSetEntry(1, create("a")))).isEqualTo(0);
        assertThat(new ZSetEntry(1, create("a"))).isEqualTo(new ZSetEntry(1, create("a")));
    }

    @Test
    void compareMinScore(){
        assertThat(new ZSetEntry(MIN_SCORE, create("a"))
                .compareTo(new ZSetEntry(2, create("a")))).isLessThan(0);
    }

    @Test
    void compareMaxScore(){
        assertThat(new ZSetEntry(MAX_SCORE, create("a"))
                .compareTo(new ZSetEntry(2, create("a")))).isGreaterThan(0);
    }


    @Test
    void compareMinLex(){
        assertThat(new ZSetEntry(1, MIN_VALUE)
                .compareTo(new ZSetEntry(1, create("a")))).isLessThan(0);
    }

    @Test
    void compareMaxLex(){
        assertThat(new ZSetEntry(1, MAX_VALUE)
                .compareTo(new ZSetEntry(1, create("Z")))).isGreaterThan(0);
    }

    @Test
    void compareWithMaxLex(){
        assertThat(new ZSetEntry(1, create("Z"))
                .compareTo(new ZSetEntry(1, MAX_VALUE))).isLessThan(0);
    }

    @Test
    void equalsHashCode() {
        EqualsVerifier.forClass(ZSetEntry.class)
                .withNonnullFields("value").verify();
    }
}