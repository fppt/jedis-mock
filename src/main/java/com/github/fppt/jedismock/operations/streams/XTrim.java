package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.SequencedMap;
import com.github.fppt.jedismock.datastructures.streams.SequencedMapIterator;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.LIMIT_OPTION_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.NOT_AN_INTEGER_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;

/**
 * XTRIM key (MAXLEN | MINID) [= | ~] threshold [LIMIT count]<br>
 * Supported options: MINID, MAXLEN, LIMIT, =<br>
 * Unsupported options: "~" - due to the fact that our implementation works as = option
 */
@RedisCommand("xtrim")
public class XTrim extends AbstractRedisOperation {
    public XTrim(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    static int trimLen(SequencedMap<?, ?> map, int threshold, int limit) {
        int numberOfEvictedNodes = 0;

        while (map.size() > threshold && numberOfEvictedNodes < limit) {
            ++numberOfEvictedNodes;
            map.removeHead();
        }

        return numberOfEvictedNodes;
    }

    static int trimID(SequencedMap<StreamId, ?> map, StreamId threshold, int limit) {
        int numberOfEvictedNodes = 0;
        SequencedMapIterator<StreamId, ?> it = map.iterator();

        while (it.hasNext() && numberOfEvictedNodes < limit) {
            if (it.next().getKey().compareTo(threshold) >= 0) {
                break;
            }

            ++numberOfEvictedNodes;
            it.remove();
        }

        return numberOfEvictedNodes;
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        /* Begin parsing arguments */
        Slice key = params().get(0);
        SequencedMap<StreamId, SequencedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();

        String criterion = params().get(1).toString(); // (MAXLEN|MINID) option
        int thresholdPosition = 2;

        String param = params().get(2).toString();

        /* Checking for "=", "~" options */
        if ("~".equals(param) || "=".equals(param)) {
            ++thresholdPosition;
        }

        boolean aproxTrim = "~".equals(param);

        int limit = map.size() + 1;

        if (params().size() > thresholdPosition + 3) {
            return Response.error(SYNTAX_ERROR);
        }

        if (params().size() > thresholdPosition + 1) {
            if ("limit".equalsIgnoreCase(params().get(thresholdPosition + 1).toString())) {
                try {
                    limit = Integer.parseInt(params().get(thresholdPosition + 2).toString());
                } catch (NumberFormatException e) {
                    return Response.error(NOT_AN_INTEGER_ERROR);
                }

                if (!aproxTrim) {
                    return Response.error(LIMIT_OPTION_ERROR);
                }
            } else {
                return Response.error(SYNTAX_ERROR);
            }
        }

        switch (criterion.toUpperCase()) {
            case "MAXLEN":
                try {
                    int threshold = Integer.parseInt(params().get(thresholdPosition).toString());
                    return Response.integer(trimLen(map, threshold, limit));
                } catch (NumberFormatException e) {
                    return Response.error(SYNTAX_ERROR);
                }
            case "MINID":
                try {
                    StreamId threshold = new StreamId(params().get(thresholdPosition));
                    return Response.integer(trimID(map, threshold, limit));
                } catch (WrongStreamKeyException e) {
                    return Response.error(e.getMessage());
                }
            default:
                return Response.error(SYNTAX_ERROR);
        }
    }
}
