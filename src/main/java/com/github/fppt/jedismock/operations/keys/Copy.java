package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;

/**
 * COPY source destination [DB destination-db] [REPLACE]
 */
@RedisCommand(value = "copy")
public class Copy implements RedisOperation {
    private final OperationExecutorState state;
    private final List<Slice> params;
    private final List<String> additionalParams;

    public Copy(OperationExecutorState state, List<Slice> params) {
        this.params = params;
        this.state = state;
        this.additionalParams = params
                .stream().skip(2).map(Slice::toString).collect(Collectors.toList());
    }

    private Integer destinationDb() {
        String previous = null;
        for (String param : additionalParams) {
            if ("db".equalsIgnoreCase(previous)) {
                return Utils.convertToInteger(param);
            } else {
                previous = param;
            }
        }
        return null;
    }

    private boolean replace() {
        return additionalParams.stream().anyMatch("replace"::equalsIgnoreCase);
    }

    @Override
    public Slice execute() {
        if (params.size() < 2) {
            return Response.error("ERR wrong number of arguments for 'copy' command");
        }
        Slice source = params.get(0);
        Slice destination = params.get(1);
        RedisBase sourceBase = state.base();
        RMDataStructure value = sourceBase.getValue(source);
        if (value == null) { // source doesn't exist
            return Response.integer(0);
        }
        Integer destinationIdx = destinationDb();
        RedisBase destinationBase =
                destinationIdx == null ?
                        state.base()
                        : state.base(destinationIdx);
        if (destinationBase.exists(destination)) {
            if (replace()) {
                destinationBase.deleteValue(destination);
            } else {
                //Key already existed
                return Response.integer(0);
            }
        }
        destinationBase.putValue(destination, value);
        Long deadline = sourceBase.getDeadline(source);
        if (deadline != null) {
            destinationBase.setDeadline(destination, deadline);
        }
        //Key copied
        return Response.integer(1);
    }
}
