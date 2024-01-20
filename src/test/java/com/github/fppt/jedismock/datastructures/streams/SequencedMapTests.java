package com.github.fppt.jedismock.datastructures.streams;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SequencedMapTests {
    @Test
    void initializeMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        assertThat(map.getHead()).isNull();
        assertThat(map.getTail()).isNull();
        assertThat(map.size()).isEqualTo(0);
    }

    @Test
    void addPairsToMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        assertThat(map.getHead()).isEqualTo(1);
        assertThat(map.getTail()).isEqualTo(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.get(1)).isEqualTo(2);
        assertThat(map.get(0)).isNull();
    }

    @Test
    void addSequenceToMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 2);
        map.append(3, 2);
        map.append(4, 2);

        map.append(4, 2);
        map.append(0, 2);
        map.append(2, 2);

        assertThat(map).hasSize(2);
    }

    @Test
    void removePairsFromMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);

        Map.Entry<Integer, Integer> entry = map.remove(0);

        assertThat(entry).isNull();
        assertThat(map.getHead()).isEqualTo(1);
        assertThat(map.getTail()).isEqualTo(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.get(1)).isEqualTo(2);

        entry = map.remove(1);

        assertThat(entry.getKey()).isEqualTo(1);
        assertThat(entry.getValue()).isEqualTo(2);

        assertThat(map.getHead()).isNull();
        assertThat(map.getTail()).isNull();
        assertThat(map.size()).isEqualTo(0);
    }

    @Test
    void getNodeTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        assertThatThrownBy(() -> map.getNextKey(0)).isInstanceOf(NullPointerException.class);
        assertThat(map.getNextKey(1)).isEqualTo(2);
        assertThat(map.getNextKey(2)).isEqualTo(3);
        assertThat(map.getNextKey(3)).isNull();

        assertThatThrownBy(() -> map.getPreviousKey(0)).isInstanceOf(NullPointerException.class);
        assertThat(map.getPreviousKey(1)).isNull();
        assertThat(map.getPreviousKey(2)).isEqualTo(1);
        assertThat(map.getPreviousKey(3)).isEqualTo(2);
    }

    @Test
    void setNextNodeTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        assertThatThrownBy(() -> map.setNextKey(0, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> map.setNextKey(1, 0)).isInstanceOf(NullPointerException.class);

        map.setNextKey(1, 3);

        assertThat(map.getNextKey(1)).isEqualTo(3);
        assertThat(map.getNextKey(2)).isEqualTo(3);
        assertThat(map.getNextKey(3)).isNull();

        assertThat(map.getPreviousKey(1)).isNull();
        assertThat(map.getPreviousKey(2)).isEqualTo(1);
        assertThat(map.getPreviousKey(3)).isEqualTo(2);
    }

    @Test
    void setPreviousNodeTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        assertThatThrownBy(() -> map.setPreviousKey(0, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> map.setPreviousKey(1, 0)).isInstanceOf(NullPointerException.class);

        map.setPreviousKey(3, 1);

        assertThat(map.getNextKey(1)).isEqualTo(2);
        assertThat(map.getNextKey(2)).isEqualTo(3);
        assertThat(map.getNextKey(3)).isNull();

        assertThat(map.getPreviousKey(1)).isNull();
        assertThat(map.getPreviousKey(2)).isEqualTo(1);
        assertThat(map.getPreviousKey(3)).isEqualTo(1);
    }

    @Test
    void addRemoveStressTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(0, 0);

        IntStream.range(1, 1_000_001).forEach(el -> {
            assertThat(map.getTail()).isEqualTo(el - 1);
            assertThat(map.getHead()).isEqualTo(0);
            map.append(el, el * 2);
            assertThat(map.getTail()).isEqualTo(el);
            assertThat(map.getHead()).isEqualTo(0);

            assertThat(map.size()).isEqualTo(el + 1);
        });

        IntStream.range(1, 500_000).forEach(el -> {
            assertThat(map.getTail()).isEqualTo(1_000_000);
            assertThat(map.getHead()).isEqualTo(0);
            map.remove(el);
            assertThat(map.getTail()).isEqualTo(1_000_000);
            assertThat(map.getHead()).isEqualTo(0);

            assertThat(map.size()).isEqualTo(1_000_001 - el);
        });

        IntStream.range(0, 249_999).map(el -> 1_000_000 - el).forEach(el -> {
            assertThat(map.getTail()).isEqualTo(el);
            assertThat(map.getHead()).isEqualTo(0);
            map.remove(el);
            assertThat(map.getTail()).isEqualTo(el - 1);
            assertThat(map.getHead()).isEqualTo(0);

            assertThat(map.size()).isEqualTo(el - 499_999);
        });

        Map.Entry<Integer, Integer> entry = map.remove(0);

        assertThat(entry.getKey()).isEqualTo(0);
        assertThat(entry.getValue()).isEqualTo(0);
        assertThat(map.size()).isEqualTo(250_002);

        IntStream.range(500_000, 749_999).forEach(el -> {
            assertThat(map.getTail()).isEqualTo(750_001);
            assertThat(map.getHead()).isEqualTo(el);

            Map.Entry<Integer, Integer> localEntry = map.remove(el);
            assertThat(localEntry.getKey()).isEqualTo(el);
            assertThat(localEntry.getValue()).isEqualTo(el * 2);

            assertThat(map.getTail()).isEqualTo(750_001);
            assertThat(map.getHead()).isEqualTo(el + 1);

            assertThat(map.size()).isEqualTo( 750_001 - el);
        });
    }

    @Test
    void removeHeadTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        assertThatCode(map::removeHead).doesNotThrowAnyException();
        assertThat(map.size()).isEqualTo(0);
        assertThat(map.getHead()).isNull();

        map.append(0, 0);
        map.append(1, 0);
        map.append(2, 0);

        assertThatCode(map::removeHead).doesNotThrowAnyException();
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.getHead()).isEqualTo(1);

        assertThatCode(map::removeHead).doesNotThrowAnyException();
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.getHead()).isEqualTo(2);

        assertThatCode(map::removeHead).doesNotThrowAnyException();
        assertThat(map.size()).isEqualTo(0);
        assertThat(map.getHead()).isNull();
    }

    @Test
    void forEachTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(0, 9);
        map.append(1, 8);
        map.append(2, 7);
        map.append(3, 6);
        map.append(4, 5);
        map.append(5, 4);
        map.append(6, 3);
        map.append(7, 2);
        map.append(8, 1);
        map.append(9, 0);

        List<Integer> list = new ArrayList<>();

        map.forEach((key, value) -> list.add(key + value));

        assertThat(list.size()).isEqualTo(map.size());

        for (int el : list) {
            assertThat(el).isEqualTo(9);
        }
    }

    @Test
    void forEachWithNullActionTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        assertThatThrownBy(
                () -> map.forEach((BiConsumer<? super Integer, ? super Integer>) null)
        ).isInstanceOf(NullPointerException.class);

        map.append(0, 9);
        map.append(1, 8);
        map.append(2, 7);
        map.append(3, 6);
        map.append(4, 5);
        map.append(5, 4);
        map.append(6, 3);
        map.append(7, 2);
        map.append(8, 1);
        map.append(9, 0);

        assertThatThrownBy(
                () -> map.forEach((BiConsumer<? super Integer, ? super Integer>) null)
        ).isInstanceOf(NullPointerException.class);
    }

    @Test
    void getForwardIteratorWithParamWhenMapIsEmptyTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();
        SequencedMapIterator<Integer, Integer> it;

        it = map.iterator(7);
        assertThat(it.hasNext()).isFalse();

        it = map.iterator(0);
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    void getForwardIteratorWithParamWhenKeyExistsTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        SequencedMapIterator<Integer, Integer> it;

        it = map.iterator(3);
        assertThat(it.hasNext()).isTrue();

        Map.Entry<Integer, Integer> entry = it.next();

        assertThat(entry.getKey()).isEqualTo(3);
        assertThat(entry.getValue()).isEqualTo(4);

        it = map.iterator(5);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(5);
        assertThat(entry.getValue()).isEqualTo(6);

        it = map.iterator(1);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(1);
        assertThat(entry.getValue()).isEqualTo(2);
    }

    @Test
    void getForwardIteratorWithParamWithNullBorderTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(5, 6);
        map.append(6, 7);
        map.append(100, 101);

        assertThatThrownBy(() -> map.iterator(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void getForwardIteratorWithParamWhenKeyDoesNotExistTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(5, 6);
        map.append(6, 7);
        map.append(100, 101);

        SequencedMapIterator<Integer, Integer> it;

        it = map.iterator(3);
        assertThat(it.hasNext()).isTrue();

        Map.Entry<Integer, Integer> entry = it.next();

        assertThat(entry.getKey()).isEqualTo(5);
        assertThat(entry.getValue()).isEqualTo(6);

        it = map.iterator(7);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(100);
        assertThat(entry.getValue()).isEqualTo(101);

        it = map.iterator(Integer.MIN_VALUE);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(1);
        assertThat(entry.getValue()).isEqualTo(2);
    }

    @Test
    void getForwardIteratorWithParamStressTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();
        SequencedMapForwardIterator<Integer, Integer> it;

        for (int i = 0; i < 5001; i += 5) {
            map.append(i, i + 3);
        }

        for (int i = 0; i < 1000; ++i) {
            int key = (int) (Math.random() * 5000);
            int correctKey = key + (key % 5 == 0 ? 0 : 5 - key % 5);
            it = map.iterator(key);
            Map.Entry<Integer, Integer> entry = it.next();
            assertThat(entry.getKey()).isEqualTo(correctKey);
            assertThat(entry.getValue()).isEqualTo(correctKey + 3);
        }
    }

    @Test
    void getReverseIteratorWithParamWithEmptyMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();
        SequencedMapIterator<Integer, Integer> it;

        it = map.reverseIterator(7);
        assertThat(it.hasNext()).isFalse();

        it = map.reverseIterator(0);
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    void getReverseIteratorWithParamWhenKeyExistsTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        SequencedMapIterator<Integer, Integer> it;

        it = map.reverseIterator(3);
        assertThat(it.hasNext()).isTrue();

        Map.Entry<Integer, Integer> entry = it.next();

        assertThat(entry.getKey()).isEqualTo(3);
        assertThat(entry.getValue()).isEqualTo(4);

        it = map.reverseIterator(5);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(5);
        assertThat(entry.getValue()).isEqualTo(6);

        it = map.reverseIterator(1);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(1);
        assertThat(entry.getValue()).isEqualTo(2);
    }

    @Test
    void getReverseIteratorWithParamWithNullBorderTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(100, 101);
        map.append(5, 6);
        map.append(6, 7);
        map.append(2, 3);
        map.append(1, 2);

        assertThatThrownBy(() -> map.reverseIterator(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void getReverseIteratorWithParamWhenKeyDoesNotExistTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(5, 6);
        map.append(6, 7);
        map.append(100, 101);

        SequencedMapIterator<Integer, Integer> it;

        it = map.reverseIterator(3);
        assertThat(it.hasNext()).isTrue();

        Map.Entry<Integer, Integer> entry = it.next();

        assertThat(entry.getKey()).isEqualTo(2);
        assertThat(entry.getValue()).isEqualTo(3);

        it = map.reverseIterator(99);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(6);
        assertThat(entry.getValue()).isEqualTo(7);

        it = map.reverseIterator(Integer.MAX_VALUE);
        assertThat(it.hasNext()).isTrue();

        entry = it.next();

        assertThat(entry.getKey()).isEqualTo(100);
        assertThat(entry.getValue()).isEqualTo(101);
    }
}
