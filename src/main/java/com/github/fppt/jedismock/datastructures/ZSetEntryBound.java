package com.github.fppt.jedismock.datastructures;

import java.util.Objects;

public class ZSetEntryBound {
    public static ZSetEntryBound MINUS_INF =
            new ZSetEntryBound(new ZSetEntry(ZSetEntry.MIN_SCORE, ""), false);
    public static ZSetEntryBound PLUS_INF =
            new ZSetEntryBound(new ZSetEntry(ZSetEntry.MAX_SCORE, ""), false);

    private final ZSetEntry bound;
    private final boolean inclusive;

    public ZSetEntryBound(ZSetEntry bound, boolean inclusive) {
        Objects.requireNonNull(bound);
        this.bound = bound;
        this.inclusive = inclusive;
    }

    public ZSetEntryBound(double score, String value, boolean inclusive) {
        this.bound = new ZSetEntry(score, value);
        this.inclusive = inclusive;
    }

    public ZSetEntry getBound() {
        return bound;
    }

    public boolean isInclusive() {
        return inclusive;
    }

}
