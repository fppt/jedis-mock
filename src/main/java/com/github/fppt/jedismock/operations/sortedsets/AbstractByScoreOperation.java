package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToDouble;
import static com.github.fppt.jedismock.Utils.convertToLong;

public abstract class AbstractByScoreOperation extends AbstractRedisOperation {
    protected static final String EXCLUSIVE_PREFIX = "(";
    private static final String LOWEST_POSSIBLE_SCORE = "-inf";
    private static final String HIGHEST_POSSIBLE_SCORE = "+inf";

    public AbstractByScoreOperation(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private double getStartScore(String start) {
        if (LOWEST_POSSIBLE_SCORE.equalsIgnoreCase(start)) {
            return Double.MIN_VALUE;
        } else if (start.startsWith(EXCLUSIVE_PREFIX)) {
            return convertToDouble(start.substring(1));
        } else {
            try {
                return Double.parseDouble(start);
            } catch (NumberFormatException e) {
                throw new WrongValueTypeException("Valid start must be a number or start with '" + EXCLUSIVE_PREFIX + "' or be equal to '"
                        + LOWEST_POSSIBLE_SCORE + "'");
            }
        }
    }

    private double getEndScore(String end) {
        if (HIGHEST_POSSIBLE_SCORE.equalsIgnoreCase(end)) {
            return Double.MAX_VALUE;
        } else if (end.startsWith(EXCLUSIVE_PREFIX)) {
            return convertToDouble(end.substring(1));
        } else {
            try {
                return Double.parseDouble(end);
            } catch (NumberFormatException e) {
                throw new WrongValueTypeException("Valid end must be a number or start with '" + EXCLUSIVE_PREFIX + "' or be equal to '"
                        + HIGHEST_POSSIBLE_SCORE + "'");
            }
        }
    }

    protected Predicate<Double> getFilterPredicate(String start, String end) {
        final double startScore = getStartScore(start);
        final Predicate<Double> compareToStart = p -> start.startsWith(EXCLUSIVE_PREFIX)
                ? p.compareTo(startScore) > 0
                : p.compareTo(startScore) >= 0;

        final double endScore = getEndScore(end);
        final Predicate<Double> compareToEnd = p -> (end.startsWith(EXCLUSIVE_PREFIX)
                ? p.compareTo(endScore) < 0
                : p.compareTo(endScore) <= 0);
        return compareToStart.and(compareToEnd);
    }

    final Slice rangeByScore(Slice start, Slice end, boolean rev) {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        if (map == null || map.isEmpty()) return
                Response.array(Collections.emptyList());

        Predicate<Double> filterPredicate = getFilterPredicate(start.toString(), end.toString());

        Stream<Map.Entry<Slice, Double>> entryStream = map.entrySet().stream()
                .filter(e -> filterPredicate.test(e.getValue()));

        boolean withScores = false;
        for (int i = 3; i < params().size(); i++) {
            String param = params().get(i).toString();
            if ("withscores".equalsIgnoreCase(param)) {
                withScores = true;
            }
            else if ("limit".equalsIgnoreCase(param)) {
                long offset = convertToLong(params().get(++i).toString());
                long count = convertToLong(params().get(++i).toString());
                entryStream = entryStream.skip(offset).limit(count);
            }
        }
        entryStream = entryStream.sorted(rev ? ZRange.zRangeComparator.reversed() : ZRange.zRangeComparator);

        Stream<Slice> result;
        if (withScores) {
            result = entryStream
                    .flatMap(e -> Stream.of(e.getKey(),
                            Slice.create(e.getValue().toString())));
        } else {
            result = entryStream
                    .map(Map.Entry::getKey);
        }
        return Response.array(result
                .map(Response::bulkString)
                .collect(Collectors.toList()));
    }
}
