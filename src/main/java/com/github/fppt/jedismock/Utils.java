package com.github.fppt.jedismock;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Closeable;
import java.util.OptionalLong;
import java.util.regex.Pattern;

/**
 * Created by Xiaolu on 2015/4/21.
 */
public class Utils {

    /**
     * The integers Redis' own {@code string2ll} accepts: no leading plus, no
     * leading zeroes, no {@code -0} and no surrounding whitespace. Java's
     * {@link Long#parseLong} is more permissive than all of these, so the
     * grammar has to be checked before parsing.
     */
    private static final Pattern REDIS_INTEGER = Pattern.compile("0|-?[1-9][0-9]*");

    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses {@code value} exactly as Redis' {@code string2ll} would, returning
     * empty rather than throwing so callers can raise the error message their
     * own command uses.
     */
    public static OptionalLong parseRedisLong(String value) {
        if (!REDIS_INTEGER.matcher(value).matches()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(value));
        } catch (NumberFormatException e) {
            //Grammatically an integer, but out of range
            return OptionalLong.empty();
        }
    }

    public static long convertToLong(String value) {
        return parseRedisLong(value).orElseThrow(() ->
                new WrongValueTypeException("ERR value is not an integer or out of range"));
    }

    public static byte convertToByte(String value) {
        try {
            byte bit = Byte.parseByte(value);
            if (bit != 0 && bit != 1) {
                throw new NumberFormatException();
            }
            return bit;
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit is not an integer or out of range");
        }
    }

    public static int convertToNonNegativeInteger(String value) {
        OptionalLong parsed = parseRedisLong(value);
        if (!parsed.isPresent() || parsed.getAsLong() < 0 || parsed.getAsLong() > Integer.MAX_VALUE) {
            throw new WrongValueTypeException("ERR bit offset is not an integer or out of range");
        }
        return (int) parsed.getAsLong();
    }

    public static int convertToInteger(String value) {
        OptionalLong parsed = parseRedisLong(value);
        if (!parsed.isPresent()
                || parsed.getAsLong() < Integer.MIN_VALUE
                || parsed.getAsLong() > Integer.MAX_VALUE) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
        return (int) parsed.getAsLong();
    }

    public static double convertToDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit offset is not a double or out of range");
        }
    }

    public static String createRegexFromGlob(String glob) {
        StringBuilder out = new StringBuilder("^");
        for (int i = 0; i < glob.length(); ++i) {
            final char c = glob.charAt(i);
            switch (c) {
                case '*':
                    out.append(".*");
                    break;
                case '?':
                    out.append('.');
                    break;
                case '.':
                    out.append("\\.");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '{':
                    out.append("\\{");
                    break;
                default:
                    out.append(c);
            }
        }
        out.append('$');
        return out.toString();
    }

    public static long toNanoTimeout(String value) {
        return (long) (convertToDouble(value) * 1_000_000_000L);
    }
}
