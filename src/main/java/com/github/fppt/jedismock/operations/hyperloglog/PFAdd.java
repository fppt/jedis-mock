package com.github.fppt.jedismock.operations.hyperloglog;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RedisCommand("pfadd")
class PFAdd extends AbstractRedisOperation {
    PFAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() throws IOException, ClassNotFoundException {
        Slice key = params().get(0);
        Slice dataSlice = base().getSlice(key);

        boolean first;

        Set<Slice> set;
        int prev;
        if (dataSlice == null) {
            set = new HashSet<>();
            first = true;
            prev = 0;
        } else {
            ObjectInputStream setBytesIn = new ObjectInputStream(
                    new ByteArrayInputStream(dataSlice.data()));
            RMSet dataSet = (RMSet) setBytesIn.readObject();
            setBytesIn.close();

            set = dataSet.getStoredData();
            first = false;
            prev = set.size();
        }

        set.addAll(params().subList(1, params().size()));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream setBytesOut = new ObjectOutputStream(byteArrayOutputStream);
        setBytesOut.writeObject(new RMSet(set));
        setBytesOut.flush();
        Slice outData = Slice.create(byteArrayOutputStream.toByteArray());

        if (first) {
            base().putSlice(key, outData);
        } else {
            base().putSlice(key, outData, null);
        }

        if (prev != set.size()) {
            return Response.integer(1L);
        }
        return Response.integer(0L);
    }
}
