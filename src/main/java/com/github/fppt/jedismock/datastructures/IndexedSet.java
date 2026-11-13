package com.github.fppt.jedismock.datastructures;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * A {@link java.util.Set} that returns a uniformly random element in O(1).
 *
 * <p>Removing an element moves the last element of the array into
 * the freed slot and updates that one element's slot in the map, so the live
 * elements always occupy {@code [0, size)} and a random index is a uniformly
 * random element. Redis gets a similar effect by sampling its own bucket table
 * ({@code dictGetFairRandomKey}).
 *
 * <p>Order is unspecified after first removal. As with {@link java.util.HashSet}, null is allowed,
 * the iterator is fail-fast.
 *
 * <p>NOT THREAD SAFE</p>
 */
public final class IndexedSet<E> extends AbstractSet<E> {
    private final Map<E, Integer> positions;
    private final ArrayList<E> elements;

    public IndexedSet() {
        positions = new HashMap<>();
        elements = new ArrayList<>();
    }

    public IndexedSet(Collection<? extends E> source) {
        positions = new HashMap<>(Math.max((int) (source.size() / 0.75f) + 1, 16));
        elements = new ArrayList<>(source.size());
        addAll(source);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean contains(Object o) {
        return positions.containsKey(o);
    }

    @Override
    public boolean add(E e) {
        if (positions.containsKey(e)) {
            return false;
        }
        positions.put(e, elements.size());
        elements.add(e);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        Integer index = positions.get(o);
        if (index == null) {
            return false;
        }
        removeAt(index);
        return true;
    }

    @Override
    public void clear() {
        positions.clear();
        elements.clear();
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    public E random(Random random) {
        if (elements.isEmpty()) {
            throw new NoSuchElementException();
        }
        return elements.get(random.nextInt(elements.size()));
    }

    public E popRandom(Random random) {
        if (elements.isEmpty()) {
            throw new NoSuchElementException();
        }
        return removeAt(random.nextInt(elements.size()));
    }

    /**
     * Returns {@code min(count, size)} distinct elements, a uniformly random
     * subset in uniformly random order, leaving the set unchanged. Runs a
     * Fisher-Yates shuffle over virtual indexes, so time and extra memory are
     * O(count) rather than O(size). A non-positive count gives an empty list.
     */
    public List<E> sampleDistinct(int count, Random random) {
        int n = elements.size();
        int k = Math.min(Math.max(count, 0), n);
        List<E> result = new ArrayList<>(k);
        // swaps.get(i) is the element index currently at virtual position i;
        // a missing key means position i still holds index i
        Map<Integer, Integer> swaps = new HashMap<>();
        for (int i = 0; i < k; i++) {
            int j = i + random.nextInt(n - i);
            int atJ = swaps.getOrDefault(j, j);
            swaps.put(j, swaps.getOrDefault(i, i));
            result.add(elements.get(atJ));
        }
        return result;
    }

    public List<E> sampleWithReplacement(int count, Random random) {
        if (count <= 0 || elements.isEmpty()) {
            return new ArrayList<>();
        }
        List<E> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(elements.get(random.nextInt(elements.size())));
        }
        return result;
    }

    private E removeAt(int index) {
        E removed = elements.get(index);
        positions.remove(removed);
        int last = elements.size() - 1;
        E moved = elements.remove(last);
        if (index != last) {
            elements.set(index, moved);
            positions.put(moved, index);
        }
        return removed;
    }

    private final class Itr implements Iterator<E> {
        private int cursor = 0;
        private int lastReturned = -1;

        @Override
        public boolean hasNext() {
            return cursor < elements.size();
        }

        @Override
        public E next() {
            if (cursor >= elements.size()) {
                throw new NoSuchElementException();
            }
            lastReturned = cursor;
            return elements.get(cursor++);
        }

        @Override
        public void remove() {
            if (lastReturned < 0) {
                throw new IllegalStateException();
            }
            removeAt(lastReturned);
            cursor = lastReturned;
            lastReturned = -1;
        }
    }
}
