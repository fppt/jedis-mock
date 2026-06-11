package com.github.fppt.jedismock.datastructures.streams;

import org.jspecify.annotations.Nullable;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

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
    private @Nullable K curr;

    /**
     * Map that iterator refers to
     */
    private final SequencedMap<K, V> map;

    public SequencedMapForwardIterator(@Nullable K curr, SequencedMap<K, V> map) {
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

        K nextKey = curr == null ? map.getHead() : map.getNextKey(curr);
        nextKey = Objects.requireNonNull(nextKey);
        curr = nextKey;

        return new AbstractMap.SimpleEntry<>(nextKey, map.get(nextKey));
    }

    void stepBack() {
        curr = map.getPreviousKey(Objects.requireNonNull(curr));
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
