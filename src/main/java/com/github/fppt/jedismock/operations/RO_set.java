package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

class RO_set extends AbstractRedisOperation {
    RO_set(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params(0);
        Slice value = params(1);

        if (nx()) {
            Slice old = base().getValue(key);
            if (old == null) {
                base().putValue(key, value, ttl());
                return Response.OK;
            } else {
                return Response.NULL;
            }
        } else if (xx()) {
            Slice old = base().getValue(key);
            if (old == null) {
                return Response.NULL;
            } else {
                base().putValue(key, value, ttl());
                return Response.OK;
            }
        } else {
            base().putValue(key, value, ttl());
            return Response.OK;
        }
    }

    private static final Slice NX = Slice.create("nx");
    private static final Slice XX = Slice.create("xx");
    private static final Slice EX = Slice.create("ex");
    private static final Slice PX = Slice.create("px");

    private boolean nx() {
        int size = params().size();
        for (int i = 0; i < size; i++) {
            if (params(i).equals(NX))
                return true;
        }
        return false;
    }

    private boolean xx() {
        int size = params().size();
        for (int i = 0; i < size; i++) {
            if (params(i).equals(XX))
                return true;
        }
        return false;
    }

    private Long ttl() {
        int size = params().size();
        for (int i = 0; i < size; i++) {
            if (params(i).equals(EX)) {
                return 1000 * Utils.convertToLong(new String(params(i + 1).data()));
            } else if (params(i).equals(PX)) {
                return Utils.convertToLong(new String(params(i + 1).data()));
            }
        }
        return null;
    }

    private Slice params(int i) {
        return params().get(i);
    }

}
