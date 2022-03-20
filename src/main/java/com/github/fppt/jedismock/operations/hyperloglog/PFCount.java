package com.github.fppt.jedismock.operations.hyperloglog;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RedisCommand("pfcount")
class PFCount extends AbstractRedisOperation {
    PFCount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() throws IOException, ClassNotFoundException {
        Set<Slice> set = new HashSet<>();
        for (Slice key : params()) {
            Slice dataSlice = base().getSlice(key);

            if (dataSlice == null) {
                continue;
            }

            ObjectInputStream objectInputStream1 = new ObjectInputStream(
                    new ByteArrayInputStream(dataSlice.data()));
            RMSet data = (RMSet) objectInputStream1.readObject();
            objectInputStream1.close();

            Set<Slice> s = data.getStoredData();
            set.addAll(s);
        }
        return Response.integer(set.size());
    }
}
