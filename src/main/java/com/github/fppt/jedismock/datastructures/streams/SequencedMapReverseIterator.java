package com.github.fppt.jedismock.datastructures.streams;

import org.jspecify.annotations.Nullable;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

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
    private @Nullable K curr;

    /**
     * Map that iterator refers to
     */
    private final SequencedMap<K, V> map;

    public SequencedMapReverseIterator(@Nullable K curr, SequencedMap<K, V> map) {
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

        K nextKey = curr == null ? map.getTail() : map.getPreviousKey(curr);
        nextKey = Objects.requireNonNull(nextKey);
        curr = nextKey;

        return new AbstractMap.SimpleEntry<>(nextKey, map.get(nextKey));
    }

    void stepBack() {
        curr = map.getNextKey(Objects.requireNonNull(curr));
    }
}
