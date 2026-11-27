package com.github.fppt.jedismock.datastructures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;


public final class RandomSampler {
    private RandomSampler() {
    }

    public static <E> List<E> sampleDistinct(Collection<? extends E> source, int count, Random random) {
        inputValidation(source, count, random);
        if (count == 0 || source.isEmpty()) {
            return Collections.emptyList();
        }
        int remaining = source.size();
        int needed = Math.min(count, remaining);
        List<E> result = new ArrayList<>(needed);
        Iterator<? extends E> iterator = source.iterator();
        // Simulation of getting k/n without division.
        while (needed > 0) {
            E member = iterator.next();
            if (random.nextInt(remaining) < needed) {
                result.add(member);
                needed--;
            }
            remaining--;
        }
        Collections.shuffle(result, random);
        return result;
    }

    public static <E> List<E> sampleWithReplacement(Collection<? extends E> source, int count, Random random) {
        inputValidation(source, count, random);
        if (count == 0 || source.isEmpty()) {
            return Collections.emptyList();
        }
        int size = source.size();
        Map<Integer, Integer> copiesByPosition = new HashMap<>();
        for (int draw = 0; draw < count; draw++) {
            copiesByPosition.merge(random.nextInt(size), 1, Integer::sum);
        }
        List<E> result = new ArrayList<>(count);
        int position = 0;
        Iterator<? extends E> iterator = source.iterator();
        while (result.size() < count) {
            E member = iterator.next();
            int copies = copiesByPosition.getOrDefault(position++, 0);
            for (int copy = 0; copy < copies; copy++) {
                result.add(member);
            }
        }
        Collections.shuffle(result, random);
        return result;
    }

    private static <E> void inputValidation(Collection<? extends E> source, int count, Random random) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(random);
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }
    }
}
