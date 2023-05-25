package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToLong;

public abstract class AbstractByScoreOperation extends AbstractRedisOperation {
    protected static final String EXCLUSIVE_PREFIX = "(";
    private static final String LOWEST_POSSIBLE_SCORE = "-inf";
    private static final String HIGHEST_POSSIBLE_SCORE = "+inf";

    public AbstractByScoreOperation(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private static double toDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("Valid start must be a number or start with '" + EXCLUSIVE_PREFIX + "' or be equal to '"
                    + LOWEST_POSSIBLE_SCORE + "'");
        }
    }

    final ZSetEntryBound getStartBound(String start) {
        if (LOWEST_POSSIBLE_SCORE.equalsIgnoreCase(start)) {
            return ZSetEntryBound.MINUS_INF;
        } else if (start.startsWith(EXCLUSIVE_PREFIX)) {
            return new ZSetEntryBound(toDouble(start.substring(1)), ZSetEntry.MAX_VALUE, false);
        } else {
            return new ZSetEntryBound(toDouble(start), ZSetEntry.MIN_VALUE, true);
        }
    }

    final ZSetEntryBound getEndBound(String end) {
        if (HIGHEST_POSSIBLE_SCORE.equalsIgnoreCase(end)) {
            return ZSetEntryBound.PLUS_INF;
        } else if (end.startsWith(EXCLUSIVE_PREFIX)) {
            return new ZSetEntryBound(toDouble(end.substring(1)), ZSetEntry.MIN_VALUE, false);
        } else {
            return new ZSetEntryBound(toDouble(end), ZSetEntry.MAX_VALUE, true);
        }
    }

    final Slice rangeByScore(Slice start, Slice end, boolean rev) {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (mapDBObj.isEmpty()) return
                Response.EMPTY_ARRAY;

        NavigableSet<ZSetEntry> subset =
                mapDBObj.subset(getStartBound(start.toString()), getEndBound(end.toString()));
        if (rev) {
            subset = subset.descendingSet();
        }
        Stream<ZSetEntry> entries = subset.stream();
        boolean withScores = false;
        for (int i = 3; i < params().size(); i++) {
            String param = params().get(i).toString();
            if ("withscores".equalsIgnoreCase(param)) {
                withScores = true;
            } else if ("limit".equalsIgnoreCase(param)) {
                long offset = convertToLong(params().get(++i).toString());
                long count = convertToLong(params().get(++i).toString());
                entries = entries.skip(offset);
                if (count >= 0) {
                    entries = entries.limit(count);
                }
            }
        }

        Stream<Slice> result;
        if (withScores) {
            result = entries
                    .flatMap(e -> Stream.of(e.getValue(),
                            Slice.create(Double.toString(e.getScore()))));
        } else {
            result = entries.map(ZSetEntry::getValue);
        }
        final List<Slice> list = result
                .map(Response::bulkString)
                .collect(Collectors.toList());

        return Response.array(list);
    }
}
