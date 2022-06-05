package com.github.fppt.jedismock.datastructures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RMZSetTest {
    @Test
    void compareScore() {
        assertTrue(new ZSetEntry(1, "a").compareTo(new ZSetEntry(2, "a")) < 0);
    }

    @Test
    void compareLex() {
        assertTrue(new ZSetEntry(1, "a").compareTo(new ZSetEntry(1, "b")) < 0);
    }

    @Test
    void compareEquals() {
        assertEquals(0, new ZSetEntry(1, "a").compareTo(new ZSetEntry(1, "a")));
        assertEquals(new ZSetEntry(1, "a"), new ZSetEntry(1, "a"));
    }

    @Test
    void compareMinScore(){
        assertTrue(new ZSetEntry(ZSetEntry.MIN_SCORE, "a")
                .compareTo(new ZSetEntry(2, "a")) < 0);
    }

    @Test
    void compareMaxScore(){
        assertTrue(new ZSetEntry(ZSetEntry.MAX_SCORE, "a")
                .compareTo(new ZSetEntry(2, "a")) > 0);
    }


    @Test
    void compareMinLex(){
        assertTrue(new ZSetEntry(1, ZSetEntry.MIN_VALUE)
                .compareTo(new ZSetEntry(1, "a")) < 0);
    }

    @Test
    void compareMaxLex(){
        assertTrue(new ZSetEntry(1, ZSetEntry.MAX_VALUE)
                .compareTo(new ZSetEntry(1, "Z")) > 0);
    }

    @Test
    void compareWithMaxLex(){
        assertTrue(new ZSetEntry(1, "Z")
                .compareTo(new ZSetEntry(1, ZSetEntry.MAX_VALUE)) < 0);
    }
}