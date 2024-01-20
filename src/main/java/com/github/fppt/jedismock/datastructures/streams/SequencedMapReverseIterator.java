package com.github.fppt.jedismock.datastructures.streams;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Reverse iterator for {@link SequencedMap SequencedMap}
 *
 * @param <K> keys type, must implement {@link java.lang.Comparable Comparable}
 * @param <V> values type
 */
public class SequencedMapReverseIterator<K extends Comparable<K>, V> implements SequencedMapIterator<K, V> {
    /**
     * Iterator takes place after this key. If is {@code null} then iterator takes place after the tail of the map.
     */
    private K curr;

    /**
     * Map that iterator refers to
     */
    private final SequencedMap<K, V> map;

    public SequencedMapReverseIterator(K curr, SequencedMap<K, V> map) {
        this.map = map;
        this.curr = curr == null ? null : map.getNextKey(curr); // null is possible when map.size == 0
    }

    @Override
    public boolean hasNext() {
        if (curr == null) {
            return map.getTail() != null;
        }

        return map.getPreviousKey(curr) != null;
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There is no elements left");
        }

        if (curr == null) {
            curr = map.getTail();
        } else {
            curr = map.getPreviousKey(curr);
        }

        return new AbstractMap.SimpleEntry<>(curr, map.get(curr));
    }

    void stepBack() {
        curr = map.getNextKey(curr);
    }
}
