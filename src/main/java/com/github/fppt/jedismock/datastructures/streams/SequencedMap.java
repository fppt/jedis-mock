package com.github.fppt.jedismock.datastructures.streams;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An associative array with O(1) get, delete operations.<br>
 * Can be interpreted as a sequence of nodes that allows to iterate map.
 *
 * @param <K> keys type, must implement {@link java.lang.Comparable Comparable}
 * @param <V> values type
 */
public class SequencedMap<K extends Comparable<K>, V> implements Iterable<Map.Entry<K, V>> {
    /**
     * A node that replaces value in {@code HashMap}. Contains additional data of next and previous nodes.
     */
    protected class LinkedMapNode {
        protected final V value;
        protected K next;
        protected K prev;

        LinkedMapNode(V value) {
            this.value = value;
        }

        public LinkedMapNode setNext(K next) {
            this.next = next;
            return this;
        }

        public LinkedMapNode setPrev(K prev) {
            this.prev = prev;
            return this;
        }
    }

    private final Map<K, LinkedMapNode> map;
    private K tail;
    private K head;
    private int size;

    public SequencedMap() {
        this.map = new HashMap<>();
        size = 0;
    }

    /**
     * Add a mapping to the end of {@code SequencedMap}
     * @asymptotic O(1)
     * @param key the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     */
    public void append(K key, V value) {
        if (size == 0) {
            head = key; // the map is empty, so the first appended element becomes the head
        } else {
            map.get(tail).setNext(key); // the map is not empty, so we have to update the reference
        }

        map.put(key, new LinkedMapNode(value).setPrev(tail));


        ++size;
        tail = key;
    }

    /**
     * Remove the mapping for the given key from map if it exists
     * @asymptotic O(1) regardless the size of map
     * @param key the key of mapping to be removed
     * @return deleted entry if a mapping for the key exists otherwise {@code null}
     */
    public Map.Entry<K, V> remove(K key) {
        if (!map.containsKey(key)) {
            return null;
        }

        if (size == 1) {
            size = 0;
            tail = null;
            head = null;

            return new AbstractMap.SimpleEntry<>(key, map.remove(key).value);
        }

        if (key.equals(tail)) {
            tail = getPreviousKey(key);
            map.get(tail).next = null;
        } else if (key.equals(head)) {
            head = getNextKey(key);
            map.get(head).prev = null;
        } else {
            setNextKey(getPreviousKey(key), getNextKey(key));
            setPreviousKey(getNextKey(key), getPreviousKey(key));
        }

        --size;
        return new AbstractMap.SimpleEntry<>(key, map.remove(key).value);
    }

    /**
     * Remove the mapping for the first key from map if it exists
     * @asymptotic O(1) regardless the size of map
     */
    public void removeHead() {
        if (size == 0) {
            return;
        }

        --size;
        head = getNextKey(head);
    }

    /**
     * Get the value to which the given key is mapped
     * @asymptotic O(1)
     * @param key the key whose associated value is to be returned
     * @return the value to which the given key is mapped or {@code null} if map does not contain it
     */
    public V get(K key) {
        LinkedMapNode node = map.get(key);

        if (node == null) {
            return null;
        }

        return node.value;
    }

    /**
     * Get the size of map
     * @return existing mappings count
     */
    public int size() {
        return size;
    }

    /**
     * Get the key of the next node. If there is no mapping for the provided key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code SequencedMapIterator}
     *
     * @param key the key of the node whose following key is being searched for
     * @return the key of the node that follows the given one
     */
    K getNextKey(K key) {
        return map.get(key).next;
    }

    /**
     * Set the key of the next node. If there is no mapping for the provided key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code SequencedMapIterator}
     *
     * @param key the key of the node whose following key is being updated
     * @param next the key of the node which is to follow the given one
     */
    void setNextKey(K key, K next) {
        if (!map.containsKey(next) || !map.containsKey(key)) {
            throw new NullPointerException("Map does not contain some of provided keys");
        }

        map.get(key).next = next;
    }

    /**
     * Get the key of the previous node. If there is no mapping for the key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code SequencedMapIterator}
     *
     * @param key the key of the node whose previous key is being searched for
     * @return the key of the node that precedes the given one
     */
    public K getPreviousKey(K key) {
        return map.get(key).prev;
    }

    /**
     * Set the key of the previous node. If there is no mapping for the provided key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code SequencedMapIterator}
     *
     * @param key the key of the node whose previous key is being updated
     * @param prev the key of the node which is to precede the given one
     */
    public void setPreviousKey(K key, K prev) {
        if (!map.containsKey(prev) || !map.containsKey(key)) {
            throw new NullPointerException("Map does not contain some of provided keys");
        }

        map.get(key).prev = prev;
    }

    /**
     * Checks whether a mapping for the given key exists.
     *
     * @return {@code true} if the mapping exists otherwise {@code false}
     */
    public boolean contains(K key) {
        return map.containsKey(key);
    }

    /**
     * Get the key of the first mapping.
     *
     * @return the first node key in the sequence
     */
    public K getHead() {
        return head;
    }

    /**
     * Get the key of the last mapping.
     *
     * @return the last node key in the sequence
     */
    public K getTail() {
        return tail;
    }

    /**
     * Get {@link SequencedMapForwardIterator SequencedMapIterator}
     * whose iteration starts from the head node
     *
     * @return iterator that allows to iterate map
     */
    @Override
    public SequencedMapForwardIterator<K, V> iterator() {
        return new SequencedMapForwardIterator<>(head, this);
    }

    /**
     * Get {@link SequencedMapForwardIterator SequencedMapForwardIterator}
     * whose iteration starts from the provided key if there is a mapping for it otherwise
     * iteration starts from the closest higher element.
     * If {@code key} is {@code null} than {@code NullPointerException} is thrown.
     *
     * @param key the key which is the start of iteration
     * @return iterator that points to the provided key or the closest higher key
     */
    public SequencedMapForwardIterator<K, V> iterator(K key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        if (map.containsKey(key)) {
            return new SequencedMapForwardIterator<>(key, this);
        }

        // failed to get -> searching the first suitable element (potentially can be replaced with binary search)
        K startKey = null;
        SequencedMapForwardIterator<K, V> it = new SequencedMapForwardIterator<>(null, this);

        if (key != head) { // searching the first node
            while (it.hasNext()) {
                startKey = it.next().getKey();

                if (startKey.compareTo(key) >= 0) {
                    break;
                }
            }
        }

        if (startKey != null) { // map might be empty
            it.stepBack();
        }

        return it;
    }

    /**
     * Get {@link SequencedMapReverseIterator SequencedMapReverseIterator}
     * whose iteration starts from the tail
     *
     * @return iterator that allows to iterate map in reversed order
     */
    public SequencedMapReverseIterator<K, V> reverseIterator() {
        return new SequencedMapReverseIterator<>(tail, this);
    }

    /**
     * Get {@link SequencedMapReverseIterator SequencedMapReverseIterator}
     * whose iteration starts from the provided key if there is a mapping for it otherwise
     * iteration starts from the closest lower element.
     * If {@code key} is {@code null} than {@code NullPointerException} is thrown.
     *
     * @param key the key which is the start of iteration
     * @return iterator that points to the provided key or the closest lower key
     */
    public SequencedMapReverseIterator<K, V> reverseIterator(K key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        if (map.containsKey(key)) {
            return new SequencedMapReverseIterator<>(key, this);
        }

        // failed to get -> searching the first suitable element (potentially can be replaced with binary search)
        K startKey = null;
        SequencedMapReverseIterator<K, V> it = new SequencedMapReverseIterator<>(null, this);

        if (key != tail) { // searching the first node
            while (it.hasNext()) {
                startKey = it.next().getKey();

                if (startKey.compareTo(key) <= 0) {
                    break;
                }
            }
        }

        if (startKey != null) { // map might be empty
            it.stepBack();
        }

        return it;
    }

    /**
     * Performs the given action for each element of the map.<br>
     * Method's behaviour:
     * <pre>{@code
     * for (Map.entry<K, V> entry: map) {
     *      action.accept(entry.getKey(), entry.getValue());
     * }
     * }</pre>
     * @param action function to be executed for each element of the map
     */
    public void forEach(BiConsumer<? super K, ? super V> action) {
        if (action == null) {
            throw new NullPointerException();
        }

        if (size == 0) {
            return;
        }

        K currKey = head;

        do {
            action.accept(currKey, map.get(currKey).value);
            currKey = map.get(currKey).next;
        } while (currKey != null);
    }
}
