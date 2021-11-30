package com.github.fppt.jedismock.operations.connection;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand(value = "select", transactional = false)
public class Select implements RedisOperation {
    private OperationExecutorState state;
    private List<Slice> params;

    public Select(OperationExecutorState state, List<Slice> params){
        this.params = params;
        this.state = state;
    }

    @Override
    public Slice execute() {
        int selectedRedisBase = Integer.parseInt(new String(params.get(0).data()));
        state.changeActiveRedisBase(selectedRedisBase);
        return Response.OK;
    }
}
