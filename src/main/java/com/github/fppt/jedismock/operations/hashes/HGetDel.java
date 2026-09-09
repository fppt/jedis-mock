package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("hgetdel")
class HGetDel extends AbstractRedisOperation {
    HGetDel(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 4;
    }

    protected Slice response() {
        Slice key = params().get(0);

        if (!"FIELDS".equalsIgnoreCase(params().get(1).toString())) {
            return Response.error("ERR Mandatory argument FIELDS is missing or not at the right position");
        }

        int numFields = 0;
        try {
            numFields = Integer.parseUnsignedInt(params().get(2).toString());
        } catch (NumberFormatException ignored) {
            // do nothing. Even when the number format is wrong, "real" Redis says
            // "Number of fields must be a positive integer".
        }
        if (numFields < 1) {
            return Response.error("ERR Number of fields must be a positive integer");
        }
        List<Slice> fields = params().subList(3, params().size());
        if (numFields != fields.size()) {
            return Response.error("ERR The `numfields` parameter must match the number of arguments");
        }

        boolean somethingWasDeleted = false;
        List<Slice> output = new ArrayList<>();
        for (Slice field : fields) {
            Slice oldValue = base().getSlice(key, field);
            base().deleteValue(key, field);
            output.add(Response.bulkString(oldValue));
            if (oldValue != null) {
                somethingWasDeleted = true;
            }
        }

        if (somethingWasDeleted) {
            base().notifyKeyspaceEvent(KeyspaceEvent.HDEL, key);
            // Deleting the last field removes the hash, a generic del
            if (!base().exists(key)) {
                base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
            }
        }

        return Response.array(output);
    }
}
