package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.function.Predicate;

import static com.github.fppt.jedismock.Utils.convertToDouble;

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
}
