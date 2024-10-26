package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Optional;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("setrange")
public class SetRange extends AbstractRedisOperation {
    public SetRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        int offset = convertToInteger(params().get(1).toString());
        Slice value = params().get(2);
        String oldValue = Optional.ofNullable(base().getRMString(key))
                .map(RMString::getStoredDataAsString)
                .orElse("");
        String padding = "";
        if (offset > oldValue.length()) {
            padding = new String(new byte[offset - oldValue.length()]);
        }
        String newValue =
                oldValue.substring(0, Math.min(offset, oldValue.length()))
                        + padding
                        + value.toString();
        if (offset + value.length() < oldValue.length()) {
            newValue += oldValue.substring(offset + value.length());
        }
        if (!oldValue.equals(newValue)) {
            base().putValue(key, Slice.create(newValue).extract(), null);
        }
        return Response.integer(newValue.length());
    }
}
