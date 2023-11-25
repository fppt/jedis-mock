package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

public abstract class AbstractByScoreOperation extends AbstractRedisOperation {

    public AbstractByScoreOperation(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    public static double toDouble(String value) {
        if ("nan".equalsIgnoreCase(value)) {
            throw new ArgumentException("*ERR*weight*not*float*");
        }
        if ("+inf".equalsIgnoreCase(value)) {
            return Double.POSITIVE_INFINITY;
        }
        if ("-inf".equalsIgnoreCase(value)) {
            return Double.NEGATIVE_INFINITY;
        }

        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("*ERR*not*float*");
        }
    }


    public Double getSum(Double score, String increment) {
        if ("+inf".equalsIgnoreCase(increment) || "infinity".equalsIgnoreCase(increment)) {
            if (score == Double.NEGATIVE_INFINITY) {
                throw new ArgumentException("ERR resulting score is not a number (NaN)");
            } else {
                return Double.POSITIVE_INFINITY;
            }
        } else if ("-inf".equalsIgnoreCase(increment) || "-infinity".equalsIgnoreCase(increment)) {
            if (score == Double.POSITIVE_INFINITY) {
                throw new ArgumentException("ERR resulting score is not a number (NaN)");
            } else {
                return Double.NEGATIVE_INFINITY;
            }
        } else if (score.isInfinite()) {
            return score;
        } else {
            return score + toDouble(increment);
        }
    }
}
