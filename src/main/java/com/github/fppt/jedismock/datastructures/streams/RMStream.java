package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;

import static java.lang.Long.compareUnsigned;
import static java.lang.Long.parseUnsignedLong;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.TOP_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.ID_OVERFLOW_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.INVALID_ID_ERROR;
import static java.lang.Long.toUnsignedString;

public class RMStream implements RMDataStructure {
    private final SequencedMap<StreamId, SequencedMap<Slice, Slice>> storedData;
    private StreamId lastId;


    public RMStream() {
        storedData = new SequencedMap<>();
        lastId = new StreamId();
    }

    public StreamId getLastId() {
        return lastId;
    }

    public SequencedMap<StreamId, SequencedMap<Slice, Slice>> getStoredData() {
        return storedData;
    }

    public void updateLastId(StreamId id) {
        lastId = id;
    }

    public Slice replaceAsterisk(Slice key) throws WrongStreamKeyException {
        if (key.toString().equals("*")) {
            /* 0xFFFFFFFFFFFFFFFF-0xFFFFFFFFFFFFFFFF is already in use - overflow */
            if (lastId.getFirstPart() == -1 && lastId.getSecondPart() == -1) {
                throw new WrongStreamKeyException(ID_OVERFLOW_ERROR);
            }

            long secondPart = lastId.getSecondPart() + 1;
            long firstPart = lastId.getFirstPart() + (lastId.getSecondPart() == -1 ? 1 : 0);

            return Slice.create(toUnsignedString(firstPart) + "-" + toUnsignedString(secondPart));
        }

        String[] parsedKey = key.toString().split("-");

        if (parsedKey.length != 2) {
            return key; /* Wrong key format - will be caught when creating StreamId */
        }

        try {
            if (compareUnsigned(lastId.getFirstPart(), parseUnsignedLong(parsedKey[0])) > 0) {
                throw new WrongStreamKeyException(TOP_ERROR);
            }

            if (parsedKey[1].equals("*")) {
                if (compareUnsigned(lastId.getFirstPart(), parseUnsignedLong(parsedKey[0])) == 0) {
                    /* The second part is 0xFFFFFFFFFFFFFFFF - overflow */
                    if (lastId.getSecondPart() == -1) {
                        throw new WrongStreamKeyException(TOP_ERROR);
                    }

                    /* Incrementing the second part */
                    return Slice.create(parsedKey[0] + "-" + toUnsignedString(lastId.getSecondPart() + 1));
                }

                /* Use 0 as the smallest unsigned long number */
                return Slice.create(parsedKey[0] + "-0");
            }
        } catch (NumberFormatException e) {
            throw new WrongStreamKeyException(INVALID_ID_ERROR);
        }

        return key;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMStream value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "stream";
    }
}
