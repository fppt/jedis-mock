package com.github.fppt.jedismock.datastructures;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RMZSetTest {
    @Test
    void compareScore() {
        assertTrue(new ZSetEntry(1, Slice.create("a"))
                .compareTo(new ZSetEntry(2, Slice.create("a"))) < 0);
    }

    @Test
    void compareLex() {
        assertTrue(new ZSetEntry(1, Slice.create("a"))
                .compareTo(new ZSetEntry(1, Slice.create("b"))) < 0);
    }

    @Test
    void compareEquals() {
        assertEquals(0, new ZSetEntry(1, Slice.create("a"))
                .compareTo(new ZSetEntry(1, Slice.create("a"))));
        assertEquals(new ZSetEntry(1, Slice.create("a")), new ZSetEntry(1, Slice.create("a")));
    }

    @Test
    void compareMinScore(){
        assertTrue(new ZSetEntry(ZSetEntry.MIN_SCORE, Slice.create("a"))
                .compareTo(new ZSetEntry(2, Slice.create("a"))) < 0);
    }

    @Test
    void compareMaxScore(){
        assertTrue(new ZSetEntry(ZSetEntry.MAX_SCORE, Slice.create("a"))
                .compareTo(new ZSetEntry(2, Slice.create("a"))) > 0);
    }


    @Test
    void compareMinLex(){
        assertTrue(new ZSetEntry(1, ZSetEntry.MIN_VALUE)
                .compareTo(new ZSetEntry(1, Slice.create("a"))) < 0);
    }

    @Test
    void compareMaxLex(){
        assertTrue(new ZSetEntry(1, ZSetEntry.MAX_VALUE)
                .compareTo(new ZSetEntry(1, Slice.create("Z"))) > 0);
    }

    @Test
    void compareWithMaxLex(){
        assertTrue(new ZSetEntry(1, Slice.create("Z"))
                .compareTo(new ZSetEntry(1, ZSetEntry.MAX_VALUE)) < 0);
    }

    @Test
    void equalsHashCode() {
        EqualsVerifier.forClass(ZSetEntry.class)
                .withNonnullFields("value").verify();
    }
}