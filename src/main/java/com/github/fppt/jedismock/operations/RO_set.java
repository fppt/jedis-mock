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

        if(nx()){
            Slice old = base().getValue(key);
            if(old == null){
                base().putValue(key, value, ttl());
                return Response.OK;
            } else {
                return Response.NULL;
            }
        }

        else if(xx()){
            Slice old = base().getValue(key);
            if(old == null){
                return Response.NULL;
            } else {
                base().putValue(key, value, ttl());
                return Response.OK;
            }
        }

        else {
            base().putValue(key, value, ttl());
            return Response.OK;
        }
    }

    private static final Slice NX = Slice.create("nx");
    private static final Slice XX = Slice.create("xx");
    private static final Slice EX = Slice.create("ex");
    private static final Slice PX = Slice.create("px");

    private boolean nx(){
        int size = params().size();

        // SET key value NX
        if(size== 3) {
            return params(2).equals(NX);
        }

        // SET key value (EX s | PX ms) NX
        if(5 <= size) {
            return params(4).equals(NX);
        }

        return false;
    }

    private boolean xx(){
        int size = params().size();

        // SET key value XX
        if(size == 3) {
            return params(2).equals(XX);
        }

        // SET key value (EX s | PX ms) XX
        if(5 <= size) {
            return params(4).equals(XX);
        }

        return false;
    }

    private Long ttl(){
        int size = params().size();
        if(4 <= size) {
            Slice marker = params(2);
            String s = new String(params(3).data());
            // SET key value EX s
            if (marker.equals(EX)) {
                return 1000 * Utils.convertToLong(s);
            }
            // SET key value PX ms
            if(marker.equals(PX)) {
                return Utils.convertToLong(s);
            }
        }
        return null;
    }

    private Slice params(int i){return params().get(i);}

}
