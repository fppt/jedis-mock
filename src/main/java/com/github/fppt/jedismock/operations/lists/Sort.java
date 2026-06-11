package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This sort implementation currently does not implement parameters BY and GET
 * moreover the complexity is currently O(N*log(N)) where N is the number of elements in the list
 */
@RedisCommand("sort")
public class Sort extends AbstractRedisOperation {
    private static final String LIMIT_PARAM = "LIMIT";
    private static final String ALPHA_PARAM = "ALPHA";
    private static final String STORE_PARAM = "STORE";
    private static final String DESC_PARAM = "DESC";

    private final Object lock;

    private boolean sortNumerically = true;
    private @Nullable Slice storeTo = null;
    private int offset = 0;
    private int count = Integer.MAX_VALUE;
    private int compareMultiplier = 1;

    public Sort(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        parseArgs();

        Slice[] items = getItems(key);

        try {
            Arrays.sort(items, this::compare);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("ERR One or more scores can't be converted into double");
        }

        List<Slice> sorted = Arrays.stream(items)
                .skip(offset)
                .limit(count)
                .collect(Collectors.toList());

        if (storeTo != null) {
            base().putValue(storeTo, new RMList(sorted));
            lock.notifyAll();
            return Response.integer(sorted.size());
        }

        return Response.array(sorted.stream().map(Response::bulkString).collect(Collectors.toList()));
    }

    private Slice[] getItems(Slice key) {
        RMDataStructure dataStructure = base().getValue(key);

        if (dataStructure instanceof RMList) {
            return ((RMList) dataStructure).getStoredData().toArray(new Slice[0]);
        }

        if (dataStructure instanceof RMSet) {
            return ((RMSet) dataStructure).getStoredData().toArray(new Slice[0]);
        }

        if (dataStructure instanceof RMZSet) {
            return ((RMZSet) dataStructure).entries(false).stream().map(ZSetEntry::getValue).toArray(Slice[]::new);
        }

        throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    private int compare(Slice a, Slice b) {
        return (sortNumerically ?
                Double.compare(Utils.convertToDouble(a.toString()), Utils.convertToDouble(b.toString())) :
                a.compareTo(b)) * compareMultiplier;
    }

    private void parseArgs() {
        List<Slice> params = params();
        for (int i = 1; i < params.size(); ++i) {
            if (ALPHA_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                sortNumerically = false;
            }

            if (STORE_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                storeTo = params.get(i + 1);
                ++i;
            }

            if (LIMIT_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                offset = Math.max(Utils.convertToInteger(params.get(i + 1).toString()), 0);
                count = Utils.convertToInteger(params.get(i + 2).toString());
                i += 2;
            }

            if (DESC_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                compareMultiplier = -1;
            }
        }
    }
}
