package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.storage.RedisBase;

import java.math.BigDecimal;
import java.util.List;

public abstract class AbstractByScoreOperation extends AbstractRedisOperation {

    /**
     * Largest magnitude Redis renders as an integer: its integer fast path only accepts
     * values within LLONG_MAX / 2.
     */
    private static final double MAX_INTEGER_SCORE = 0x1p62;

    public AbstractByScoreOperation(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    /**
     * Formats a score the way Redis renders it in a reply: {@code inf} / {@code -inf} for infinities,
     * integer digits for whole numbers up to 2^62 in magnitude, and otherwise the shortest decimal
     * that round-trips, laid out as Redis's fpconv_dtoa does (plain or exponent form).
     */
    protected static Slice scoreToSlice(double score) {
        return Slice.create(formatScore(score));
    }

    static String formatScore(double score) {
        if (Double.isInfinite(score)) {
            return score > 0 ? "inf" : "-inf";
        }
        if (score % 1 == 0 && Math.abs(score) <= MAX_INTEGER_SCORE) {
            // the cast is exact here and turns -0.0 into 0, as Redis does
            return Long.toString((long) score);
        }
        return shortestRepresentation(score);
    }

    private static String shortestRepresentation(double score) {
        BigDecimal decimal = new BigDecimal(Double.toString(Math.abs(score))).stripTrailingZeros();
        String digits = decimal.unscaledValue().toString();
        int digitCount = digits.length();
        // |score| = digits * 10^exponent
        int exponent = -decimal.scale();
        // decimal exponent of the leading digit, as it would appear after 'e'
        int magnitude = Math.abs(exponent + digitCount - 1);
        StringBuilder result = new StringBuilder();
        if (score < 0) {
            result.append('-');
        }
        if (exponent >= 0 && magnitude < digitCount + 7) {
            result.append(digits);
            for (int i = 0; i < exponent; i++) {
                result.append('0');
            }
        } else if (exponent < 0 && (exponent > -7 || magnitude < 4)) {
            int integerDigits = digitCount + exponent;
            if (integerDigits <= 0) {
                result.append("0.");
                for (int i = integerDigits; i < 0; i++) {
                    result.append('0');
                }
                result.append(digits);
            } else {
                result.append(digits, 0, integerDigits).append('.').append(digits, integerDigits, digitCount);
            }
        } else {
            result.append(digits.charAt(0));
            if (digitCount > 1) {
                result.append('.').append(digits, 1, digitCount);
            }
            result.append('e').append(exponent + digitCount - 1 < 0 ? '-' : '+').append(magnitude);
        }
        return result.toString();
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
