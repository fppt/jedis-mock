package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import static java.util.Collections.emptyNavigableSet;
import static java.util.Collections.unmodifiableNavigableSet;


public class RMZSet implements RMDataStructure {
    private final Map<Slice, Double> scores = new HashMap<>();
    private final NavigableSet<ZSetEntry> entries = new TreeSet<>();

    public Double put(Slice value, double score) {
        Double previous = scores.put(value, score);
        if (previous != null) {
            entries.remove(new ZSetEntry(previous, value));
        }
        entries.add(new ZSetEntry(score, value));
        return previous;
    }

    public Double getScore(Slice value) {
        return scores.get(value);
    }

    public boolean remove(Slice value) {
        final Double previous = scores.remove(value);
        if (previous == null) {
            return false;
        } else {
            entries.remove(new ZSetEntry(previous, value));
            return true;
        }
    }

    public int size() {
        return scores.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public NavigableSet<ZSetEntry> subset(ZSetEntryBound start,
                                          ZSetEntryBound end) {
        if (start.getBound().compareTo(end.getBound()) > 0) {
            return emptyNavigableSet();
        } else {
            return unmodifiableNavigableSet(entries.subSet(start.getBound(), start.isInclusive(),
                    end.getBound(), end.isInclusive()));
        }
    }

    public NavigableSet<ZSetEntry> entries(boolean reversed) {
        return unmodifiableNavigableSet(reversed ? entries.descendingSet() : entries);
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMZSet value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "zset";
    }

}
