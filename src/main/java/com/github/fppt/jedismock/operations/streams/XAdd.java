package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.SequencedMap;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.LIMIT_OPTION_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.TOP_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.ZERO_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.NOT_AN_INTEGER_ERROR;
import static com.github.fppt.jedismock.operations.streams.XTrim.trimID;
import static com.github.fppt.jedismock.operations.streams.XTrim.trimLen;

/**
 * XADD key [NOMKSTREAM] [(MAXLEN | MINID) [= | ~] threshold
 *   [LIMIT count]] (* | id) field value [field value ...]<br>
 * Supported options: all, except '~'<br>
 * About trim options see {@link XTrim}
 */
@RedisCommand("xadd")
public class XAdd extends AbstractRedisOperation {
    public XAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    void validate(StreamId key) throws WrongStreamKeyException {
        if (key.isZero()) {
            throw new WrongStreamKeyException(ZERO_ERROR);
        }

        StreamId lastId = getStreamFromBaseOrCreateEmpty(params().get(0)).getLastId();
        if (key.compareTo(lastId) <= 0) {
            throw new WrongStreamKeyException(TOP_ERROR);
        }
    }

    @Override
    protected int minArgs() {
        return 4;
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        RMStream stream = getStreamFromBaseOrCreateEmpty(key);
        SequencedMap<StreamId, SequencedMap<Slice, Slice>> map = stream.getStoredData();

        int idInd = 1; // 'id' index

        /* Parsing NOMSTREAM option */
        if ("nomkstream".equalsIgnoreCase(params().get(1).toString())) {
            if (!base().exists(key)) {
               return Response.NULL;
            }

            idInd++; // incrementing position
        }

        /*  Begin trim options parsing */
        String criterion = ""; // (MAXLEN|MINID) option
        int thresholdPosition = idInd + 1;
        int limit = map.size() + 1;

        String param = params().get(idInd).toString();
        if ("maxlen".equalsIgnoreCase(param) || "minid".equalsIgnoreCase(param)) {
            criterion = params().get(idInd++).toString();

            param = params().get(idInd++).toString();

            boolean approxTrim = "~".equals(param);

            if ("~".equals(param) || "=".equals(param)) {
                ++thresholdPosition;
                ++idInd;
            }

            if ("limit".equalsIgnoreCase(params().get(idInd).toString())) {
                try {
                    limit = Integer.parseInt(params().get(++idInd).toString());
                } catch (NumberFormatException e) {
                    return Response.error(NOT_AN_INTEGER_ERROR);
                }

                if (!approxTrim) {
                    return Response.error(LIMIT_OPTION_ERROR);
                }

                ++idInd;
            }
        }
        /*  End trim options parsing */

        Slice id = params().get(idInd++);


        StreamId nodeId;
        try {
           nodeId = new StreamId(stream.replaceAsterisk(id));
           validate(nodeId);
        } catch (WrongStreamKeyException e) {
            return Response.error(e.getMessage());
        }

        SequencedMap<Slice, Slice> entryValues = new SequencedMap<>();
        for (int i = idInd; i < params().size(); i += 2) {
            entryValues.append(params().get(i), params().get(i + 1));
        }

        map.append(nodeId, entryValues);
        stream.updateLastId(nodeId);

        base().putValue(key, stream);

        switch (criterion.toUpperCase()) {
            case "MAXLEN":
                try {
                    int threshold = Integer.parseInt(params().get(thresholdPosition).toString());
                    trimLen(map, threshold, limit);
                } catch (NumberFormatException e) {
                    return Response.error(SYNTAX_ERROR);
                }
                break;

            case "MINID":
                try {
                    StreamId threshold = new StreamId(params().get(thresholdPosition));
                    trimID(map, threshold, limit);
                } catch (WrongStreamKeyException e) {
                    return Response.error(e.getMessage());
                }
                break;
            default:
                // ignored
        }

        return Response.bulkString(nodeId.toSlice());
    }
}
