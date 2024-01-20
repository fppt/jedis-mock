package com.github.fppt.jedismock.datastructures.streams;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SequencedMapReverseIteratorTests {
    @Test
    void createSimpleIterator() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(3, 4);
        map.append(2, 3);
        map.append(1, 2);

        SequencedMapIterator<Integer, Integer> it = map.reverseIterator();

        int iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertThat(entry.getKey()).isEqualTo(iterCount);
            assertThat(entry.getValue()).isEqualTo(iterCount + 1);
        }

        assertThat(iterCount).isEqualTo(3);
    }

    @Test
    void createComplexIterator() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(5, 6);
        map.append(4, 5);
        map.append(3, 4);
        map.append(2, 3);
        map.append(1, 2);

        SequencedMapIterator<Integer, Integer> it = map.reverseIterator(3);

        int iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertThat(entry.getKey()).isEqualTo(iterCount + 2);
            assertThat(entry.getValue()).isEqualTo(iterCount + 3);
        }

        assertThat(iterCount).isEqualTo(3);

        it = map.reverseIterator(5);

        iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertThat(entry.getKey()).isEqualTo(iterCount + 4);
            assertThat(entry.getValue()).isEqualTo(iterCount + 5);
        }

        assertThat(iterCount).isEqualTo(1);

        it = map.reverseIterator(1);

        iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertThat(entry.getKey()).isEqualTo(iterCount);
            assertThat(entry.getValue()).isEqualTo(iterCount + 1);
        }

        assertThat(iterCount).isEqualTo(5);
    }

    @Test
    void emptyMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();
        SequencedMapIterator<Integer, Integer> it = map.reverseIterator();

        assertThat(it.hasNext()).isFalse();
        assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    }
}
