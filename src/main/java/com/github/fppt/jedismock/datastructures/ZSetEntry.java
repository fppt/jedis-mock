package com.github.fppt.jedismock.datastructures;

import java.util.Objects;

public class ZSetEntry implements Comparable<ZSetEntry> {
    public final static Double MIN_SCORE = Double.NEGATIVE_INFINITY;
    public final static Double MAX_SCORE = Double.POSITIVE_INFINITY;
    public final static Slice MIN_VALUE = Slice.empty();
    //magic string that is considered to be lexicographically greater than any other string
    public final static Slice MAX_VALUE = Slice.create("2815bbb7-cacd-4ce1-ba76-11fee7937b0e");

    private final double score;

    private final Slice value;

    ZSetEntry(double score, Slice value) {
        Objects.requireNonNull(value);
        this.score = score;
        this.value = value;
    }

    public double getScore() {
        return score;
    }

    public Slice getValue() {
        return value;
    }

    @Override
    public int compareTo(ZSetEntry o) {
        int result = Double.compare(score, o.score);
        if (result == 0) {
            if (value.equals(o.value)) {
                return 0;
            } else if (MAX_VALUE.equals(value)) {
                return 1;
            } else if (MAX_VALUE.equals(o.value)) {
                return -1;
            } else {
                return value.compareTo(o.value);
            }
        } else {
            return result;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZSetEntry zSetEntry = (ZSetEntry) o;
        return Double.compare(zSetEntry.score, score) == 0 && value.equals(zSetEntry.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, value);
    }

}
