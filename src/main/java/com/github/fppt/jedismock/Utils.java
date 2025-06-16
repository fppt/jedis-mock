package com.github.fppt.jedismock;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by Xiaolu on 2015/4/21.
 */
public class Utils {

    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long convertToLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
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
        try {
            int pos = Integer.parseInt(value);
            if (pos < 0) throw new NumberFormatException("Int less than 0");
            return pos;
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit offset is not an integer or out of range");
        }
    }

    public static int convertToInteger(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
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

    public static <E> List<E> lastNElements(List<E> list, int n) {
        return list.size() < n ? list : list.subList(list.size() - n, list.size());
    }

    /**
     * Randomly shuffles the last {@code n} elements of the given list using the provided {@link Random} instance.
     * <p>
     * If {@code n} is greater than the size of the list, the entire list is shuffled.
     * This method performs a partial Fisher-Yates shuffle from the end of the list.
     *
     * @param list the list to be partially shuffled
     * @param n the number of elements from the end of the list to shuffle
     * @param r the random number generator to use for shuffling
     * @param <E> the type of elements in the list
     */
    public static <E> void shufflePartially(List<E> list, int n, Random r) {
        int length = list.size();
        if (length < n) {
            n = length;
        }
        // We don't need to shuffle the whole list
        for (int i = length - 1; i >= length - n; --i)
        {
            Collections.swap(list, i , r.nextInt(i + 1));
        }
    }

    public static long toNanoTimeout(String value) {
        return (long) (convertToDouble(value) * 1_000_000_000L);
    }
}
