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

@RedisCommand("pfmerge")
class PFMerge extends AbstractRedisOperation {
    PFMerge(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() throws IOException, ClassNotFoundException {
        Slice key = params().get(0);
        Slice dataSlice = base().getSlice(key);

        Set<Slice> set;
        if (dataSlice == null) {
            set = new HashSet<>();
        } else {
            ObjectInputStream setBytesIn = new ObjectInputStream(
                    new ByteArrayInputStream(dataSlice.data()));
            RMSet rmData = (RMSet) setBytesIn.readObject();
            setBytesIn.close();

            set = rmData.getStoredData();
        }

        for (Slice v : params().subList(1, params().size())) {
            Slice sliceToMerge = base().getSlice(v);

            if (sliceToMerge != null) {
                ObjectInputStream currInput = new ObjectInputStream(
                        new ByteArrayInputStream(sliceToMerge.data()));
                RMSet valueToMerge = (RMSet) currInput.readObject();
                currInput.close();

                Set<Slice> s = valueToMerge.getStoredData();
                set.addAll(s);
            }
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream setBytesOut = new ObjectOutputStream(byteArrayOutputStream);
        setBytesOut.writeObject(new RMSet(set));
        setBytesOut.flush();
        Slice outData = Slice.create(byteArrayOutputStream.toByteArray());
        base().putSlice(key, outData);

        return Response.OK;
    }
}
