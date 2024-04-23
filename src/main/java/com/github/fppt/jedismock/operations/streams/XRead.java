package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.SequencedMap;
import com.github.fppt.jedismock.datastructures.streams.SequencedMapIterator;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.NEGATIVE_TIMEOUT_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.XREAD_ARGS_ERROR;

/**
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id
 *   [id ...]<br>
 * All options are supported
 */
@RedisCommand("xread")
public class XRead extends AbstractRedisOperation {
    private final Object lock;
    private final boolean isInTransaction;

    public XRead(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        lock = state.lock();
        isInTransaction = state.isTransactionModeOn();
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        int streamInd = 0;
        int count;
        long blockTimeNanosec = 0;
        boolean isBlocking = false;

        if ("count".equalsIgnoreCase(params().get(streamInd).toString())) {
            count = Integer.parseInt(params().get(++streamInd).toString());
            ++streamInd;
        } else {
            count = Integer.MAX_VALUE;
        }

        if ("block".equalsIgnoreCase(params().get(streamInd).toString())) {
            blockTimeNanosec = Long.parseLong(params().get(++streamInd).toString()) * 1_000_000;
            isBlocking = true;

            if (blockTimeNanosec < 0) {
                return Response.error(NEGATIVE_TIMEOUT_ERROR);
            }

            ++streamInd;
        }

        if (!"streams".equalsIgnoreCase(params().get(streamInd++).toString())) {
            return Response.error(SYNTAX_ERROR);
        }

        /* After STREAMS args go in pairs */
        if ((params().size() - streamInd) % 2 != 0) {
            return Response.error(XREAD_ARGS_ERROR);
        }

        int streamsCount = (params().size() - streamInd) / 2;

        SequencedMap<Slice, StreamId> mapKeyToBeginEntryId = new SequencedMap<>();

        /* Mapping all stream ids */
        for (int i = 0; i < streamsCount; ++i) {
            Slice key = params().get(streamInd + i);
            Slice id = params().get(streamInd + streamsCount + i);

            try {
                if (!base().exists(key)) {
                    mapKeyToBeginEntryId.append(
                            key,
                            "$".equalsIgnoreCase(id.toString())
                                    ? new StreamId(0, 1) // lowest possible id
                                    : new StreamId(id)
                    );
                } else {
                    mapKeyToBeginEntryId.append(
                            key,
                            "$".equalsIgnoreCase(id.toString())
                                    /* last id added to stream */
                                    ? getStreamFromBaseOrCreateEmpty(key).getStoredData().getTail()
                                    : new StreamId(id)
                    );
                }
            } catch (WrongStreamKeyException e) {
                return Response.error(e.getMessage());
            }
        }

        List<Slice> output = new ArrayList<>();

        /* Blocking */
        long waitEnd = System.nanoTime() + blockTimeNanosec;
        long waitTimeNanos;

        if (isBlocking) {
            boolean updated = false; // should be unblocked after XADD was invoked
            if (blockTimeNanosec > 0) {
                try {
                    while (!isInTransaction && !updated && ((waitTimeNanos = waitEnd - System.nanoTime()) >= 0)) {
                        for (Map.Entry<Slice, StreamId> entry : mapKeyToBeginEntryId) {
                            if (base().exists(entry.getKey())
                                    && entry.getValue()
                                    .compareTo(getStreamFromBaseOrCreateEmpty(entry.getKey())
                                    .getStoredData()
                                    .getTail()) < 0) {
                                updated = true;
                                break;
                            }
                        }

                        if (waitTimeNanos / 1_000_000 < 500) {
                            lock.wait(waitTimeNanos / 1_000_000, (int) waitTimeNanos % 1_000_000);
                        } else {
                            lock.wait(500, 0);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Response.NULL;
                }
            } else {
                try {
                    while (!isInTransaction && !updated) {
                        for (Map.Entry<Slice, StreamId> entry : mapKeyToBeginEntryId) {
                            if (base().exists(entry.getKey())
                                    && getStreamFromBaseOrCreateEmpty(entry.getKey())
                                        .getStoredData()
                                        .getTail()
                                        .compareTo(entry.getValue()) > 0) {
                                updated = true;
                                break;
                            }
                        }
                        lock.wait(500, 0);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Response.NULL;
                }
            }
        }

        /* Response */
        mapKeyToBeginEntryId.forEach((key, id) -> {
            SequencedMap<StreamId, SequencedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();
            SequencedMapIterator<StreamId, SequencedMap<Slice, Slice>> it;

            if (!base().exists(key)) {
                return; // skip
            }


            if (map.getTail() == null || id.compareTo(map.getTail()) >= 0) {
                return; // skip
            }

            try {
                id = id.increment();
            } catch (WrongStreamKeyException e) {
                return; // impossible as 0xFFFFFFFFFFFFFFFF is greater or equal to all keys
            }

            it = map.iterator(id);

            List<Slice> data = new ArrayList<>();
            int addedEntries = 1;

            while (it.hasNext() && addedEntries++ <= count) {
                Map.Entry<StreamId, SequencedMap<Slice, Slice>> entry = it.next();

                List<Slice> values = new ArrayList<>();

                entry.getValue().forEach((k, v) -> {
                    values.add(Response.bulkString(k));
                    values.add(Response.bulkString(v));
                });

                data.add(Response.array(
                        Response.bulkString(entry.getKey().toSlice()),
                        Response.array(values)
                ));
            }

            output.add(Response.array(
                    Response.bulkString(key),
                    Response.array(data)
            ));
        });

        return Response.array(output);
    }
}
