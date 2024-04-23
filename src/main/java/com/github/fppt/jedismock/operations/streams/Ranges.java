package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.streams.SequencedMap;
import com.github.fppt.jedismock.datastructures.streams.SequencedMapIterator;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.NOT_AN_INTEGER_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;

public class Ranges extends AbstractRedisOperation {
    /**
     * Multiplier for comparison:<br>
     * 1 - 'xrange'<br>
     * -1 - 'xrevrange'
     */
    protected int multiplier = 1;

    Ranges(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected StreamId preprocessExclusiveBorder(StreamId key, boolean isStart) throws WrongStreamKeyException {
        if (isStart) {
            return key.increment();
        }

        return key.decrement();
    }

    protected StreamId preprocessKey(Slice key, RMStream stream, boolean isStart) throws WrongStreamKeyException {
        String rawKey = key.toString();

        if (rawKey.equals("-")) {
            return stream.getStoredData().getHead();
        } else if (rawKey.equals("+")) {
            return stream.getStoredData().getTail();
        }

        boolean exclusive = false;
        StreamId id;

        if (rawKey.charAt(0) == '(') {
            exclusive = true;
            id = new StreamId(rawKey.substring(1));
        } else {
            id = new StreamId(key);
        }

        if (exclusive) {
            id = preprocessExclusiveBorder(id, isStart);
        }

        return id;
    }

    protected Slice range() {
        RMStream stream = getStreamFromBaseOrCreateEmpty(params().get(0));
        SequencedMap<StreamId, SequencedMap<Slice, Slice>> map = stream.getStoredData();

        /* Begin parsing arguments */
        StreamId start;
        StreamId end;
        int count = map.size();

        try {
            start = preprocessKey(params().get(1), stream, true);
            end = preprocessKey(params().get(2), stream, false);
        } catch (WrongStreamKeyException e) {
            return Response.error(e.getMessage());
        }

        if (params().size() > 3) {
            if (params().size() != 5) {
                return Response.error(SYNTAX_ERROR);
            }

            if (!"count".equalsIgnoreCase(params().get(3).toString())) {
                return Response.error(SYNTAX_ERROR);
            }

            try {
                count = Integer.parseInt(params().get(4).toString());
            } catch (NumberFormatException e) {
                return Response.error(NOT_AN_INTEGER_ERROR);
            }
        }
        /* End parsing arguments */

        if (map.size() == 0) { // empty map case
            return Response.EMPTY_ARRAY;
        }

        /* Compare with the last item in map */
        if (multiplier == 1) {
            if (start.compareTo(map.getTail()) > 0) {
                return Response.EMPTY_ARRAY;
            }
        } else {
            if (start.compareTo(map.getHead()) < 0) {
                return Response.EMPTY_ARRAY;
            }
        }

        SequencedMapIterator<StreamId, SequencedMap<Slice, Slice>> it = multiplier == 1
                ? map.iterator(start)
                : map.reverseIterator(start);

        List<Slice> output = new ArrayList<>();

        int entriesAdded = 1;

        while (it.hasNext() && entriesAdded++ <= count) {
            List<Slice> entrySlice = new ArrayList<>();
            Map.Entry<StreamId, SequencedMap<Slice, Slice>> entry = it.next();

            if (multiplier * entry.getKey().compareTo(end) > 0) {
                break;
            }

            entry.getValue().forEach((key, value) -> {
                entrySlice.add(Response.bulkString(key));
                entrySlice.add(Response.bulkString(value));
            });

            output.add(Response.array(
                    Response.bulkString(entry.getKey().toSlice()),
                    Response.array(entrySlice)
            ));
        }

        return Response.array(output);
    }

    @Override
    protected Slice response() {
        return null;
    }
}
