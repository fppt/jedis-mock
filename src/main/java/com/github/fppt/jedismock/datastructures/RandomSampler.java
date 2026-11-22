package com.github.fppt.jedismock.datastructures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;


public final class RandomSampler {
    private RandomSampler() {
    }

    public static <E> List<E> sampleDistinct(Set<? extends E> source, int count, Random random) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(random);
        if (count <= 0 || source.isEmpty()) {
            return new ArrayList<>();
        }
        int remaining = source.size();
        int needed = Math.min(count, remaining);
        List<E> result = new ArrayList<>(needed);
        Iterator<? extends E> iterator = source.iterator();
        // Simulation of getting k/n without division.
        while (needed > 0 && iterator.hasNext()) {
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

    public static <E> List<E> sampleWithReplacement(Set<? extends E> source, int count, Random random) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(random);
        if (count <= 0 || source.isEmpty()) {
            return new ArrayList<>();
        }
        int size = source.size();
        Map<Integer, Integer> copiesByPosition = new HashMap<>();
        for (int draw = 0; draw < count; draw++) {
            copiesByPosition.merge(random.nextInt(size), 1, Integer::sum);
        }
        List<E> result = new ArrayList<>(count);
        int position = 0;
        for (E member : source) {
            int copies = copiesByPosition.getOrDefault(position++, 0);
            for (int copy = 0; copy < copies; copy++) {
                result.add(member);
            }
        }
        Collections.shuffle(result, random);
        return result;
    }
}
