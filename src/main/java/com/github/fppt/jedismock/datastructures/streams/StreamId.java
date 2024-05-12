package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.RANGES_END_ID_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.RANGES_START_ID_ERROR;
import static java.lang.Long.compareUnsigned;
import static java.lang.Long.parseUnsignedLong;
import static java.lang.Long.toUnsignedString;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.INVALID_ID_ERROR;

public final class StreamId implements Comparable<StreamId> {
    private final long firstPart;
    private final long secondPart;

    /* 0-0 ID for PRIVATE API usage */
    StreamId() {
        this(0, 0);
    }

    /* ID from long numbers for PRIVATE API usage */
    public StreamId(long firstPart, long secondPart) {
        this.firstPart = firstPart;
        this.secondPart = secondPart;
    }

    public StreamId(String key) throws WrongStreamKeyException {
        Matcher matcher = Pattern.compile("(\\d+)(?:-(\\d+))?").matcher(key);
        if (matcher.matches()) {
            try {
                firstPart = parseUnsignedLong(matcher.group(1));
                secondPart = matcher.group(2) == null ? 0 : parseUnsignedLong(matcher.group(2));
            } catch (NumberFormatException e) {
                throw new WrongStreamKeyException(INVALID_ID_ERROR);
            }
        } else {
            throw new WrongStreamKeyException(INVALID_ID_ERROR);
        }
    }

    public StreamId(Slice slice) throws WrongStreamKeyException {
        this(slice.toString());
    }

    public long getFirstPart() {
        return firstPart;
    }

    public long getSecondPart() {
        return secondPart;
    }

    public boolean isZero() {
        return secondPart == 0 && firstPart == 0;
    }

    public StreamId increment() throws WrongStreamKeyException {
        long second = secondPart + 1;
        long first = firstPart;

        if (compareUnsigned(second, 0) == 0) { // the previous one was 0xFFFFFFFFFFFFFFFF => update the first part
            if (compareUnsigned(first, -1) == 0) {
                throw new WrongStreamKeyException(RANGES_START_ID_ERROR);
            }

            ++first;
        }

        return new StreamId(first, second);
    }

    public StreamId decrement() throws WrongStreamKeyException {
        long second = secondPart - 1;
        long first = firstPart;

        if (compareUnsigned(second, -1) == 0) { // the previous one was 0x0000000000000000 => update the first part
            if (compareUnsigned(first, 0) == 0) {
                throw new WrongStreamKeyException(RANGES_END_ID_ERROR);
            }

            --first;
        }

        return new StreamId(first, second);
    }

    @Override
    public int compareTo(StreamId other) {
        if (other == null) {
            return 1; // occurs in blocking (when nothing was added)
        }

        int firstPartComparison = compareUnsigned(firstPart, other.firstPart);
        return firstPartComparison != 0
                ? firstPartComparison
                : compareUnsigned(secondPart, other.secondPart);
    }

    public Slice toSlice() {
        return Slice.create(toString());
    }

    @Override
    public String toString() {
        return toUnsignedString(firstPart) + "-" + toUnsignedString(secondPart);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamId)) return false;
        StreamId streamId = (StreamId) o;
        if (firstPart != streamId.firstPart) return false;
        return secondPart == streamId.secondPart;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstPart, secondPart);
    }
}
