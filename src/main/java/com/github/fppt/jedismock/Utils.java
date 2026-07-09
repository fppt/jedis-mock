package com.github.fppt.jedismock;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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

    /**
     * Performs reservoir sampling on a given collection, returning a random subset of up to {@code count} elements.
     * <p>
     * This implementation uses Algorithm L (a fast reservoir sampling method) to efficiently sample elements
     * with uniform probability without needing to store or shuffle the entire dataset.
     *
     * @param collection the input collection to sample from
     * @param count the number of elements to sample; must be non-negative and less than or equal to the size of the collection
     * @param random a {@link Random} instance used for generating random numbers
     * @param <E> the type of elements in the input collection
     * @return a list containing up to {@code count} randomly sampled elements from the collection
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public static <E> List<E> reservoirSampling(Collection<E> collection, int count, Random random) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }

        if (count == 0) {
            return Collections.emptyList();
        }

        if (count > collection.size()) {
            count = collection.size();
        }

        List<E> result = new ArrayList<>(count);
        Iterator<E> iter = collection.iterator();
        for (int i = 0; i < count; i++) {
            result.add(iter.next());
        }

        double W = Math.exp(Math.log(random.nextDouble()) / count);
        while (iter.hasNext()) {
            int skip = (int)Math.floor(Math.log(random.nextDouble()) / Math.log(1 - W));
            for (int j = 0; j < skip; j++) {
                if (!iter.hasNext()) break;
                iter.next();
            }
            if (iter.hasNext()) {
                result.set(random.nextInt(count), iter.next());
                W = W * Math.exp(Math.log(random.nextDouble()) / count);
            }
        }

        return result;
    }

    public static long toNanoTimeout(String value) {
        return (long) (convertToDouble(value) * 1_000_000_000L);
    }
}
