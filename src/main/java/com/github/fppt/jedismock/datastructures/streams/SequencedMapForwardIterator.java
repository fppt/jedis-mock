package com.github.fppt.jedismock.datastructures.streams;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterator for {@link SequencedMap SequencedMap}
 *
 * @param <K> keys type, must implement {@link java.lang.Comparable Comparable}
 * @param <V> values type
 */
public class SequencedMapForwardIterator<K extends Comparable<K>, V> implements SequencedMapIterator<K, V> {
    /**
     * Iterator takes place before this key. If is {@code null} then iterator takes place before the head of the map.
     */
    private K curr;

    /**
     * Map that iterator refers to
     */
    private final SequencedMap<K, V> map;

    public SequencedMapForwardIterator(K curr, SequencedMap<K, V> map) {
        this.map = map;
        this.curr = curr == null ? null : map.getPreviousKey(curr); // null is possible when map.size == 0
    }

    @Override
    public boolean hasNext() {
        if (curr == null) {
            return map.getHead() != null;
        }

        return map.getNextKey(curr) != null;
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There is no elements left");
        }

        if (curr == null) {
            curr = map.getHead();
        } else {
            curr = map.getNextKey(curr);
        }

        return new AbstractMap.SimpleEntry<>(curr, map.get(curr));
    }

    void stepBack() {
        curr = map.getPreviousKey(curr);
    }

    /**
     *
     */
    @Override
    public void remove() {
        if (curr == null) {
            throw new IllegalStateException("'next' method has not been invoked yet");
        } else {
            K tmp = map.getPreviousKey(curr);
            map.remove(curr);
            curr = tmp;
        }
    }
}
