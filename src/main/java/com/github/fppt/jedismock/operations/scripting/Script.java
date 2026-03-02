package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

@RedisCommand("script")
public class Script extends AbstractRedisOperation {
    private static final String SCRIPT_PARAM_ERROR = "Wrong number of arguments for SCRIPT";

    public Script(final RedisBase base, final List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        final String operation = params().get(0).toString();
        if ("LOAD".equalsIgnoreCase(operation)) {
            return load();
        } else if ("EXISTS".equalsIgnoreCase(operation)) {
            return exists();
        } else if ("FLUSH".equalsIgnoreCase(operation)) {
            base().flushCachedLuaScrips();
            return Response.OK;
        }
        return Response.error(String.format("Unsupported operation: script %s", operation));
    }

    private Slice exists() {
        List<Slice> result = new ArrayList<>();
        for (int i = 1; i < params().size(); i++) {
            result.add(
                    Response.integer(
                            base().cachedLuaScriptExists(params().get(i).toString()) ? 1 : 0));
        }
        return Response.array(result);
    }

    private Slice load() {
        if (params().size() < 2) {
            return Response.error(SCRIPT_PARAM_ERROR);
        }
        final String script = params().get(1).toString();
        final String scriptSHA = getScriptSHA(script);
        base().addCachedLuaScript(scriptSHA, script);
        return Response.bulkString(Slice.create(scriptSHA));
    }

    public static String getScriptSHA(final String script) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA1");
            final byte[] scriptSHA1 = sha1.digest(script.getBytes());
            return byteArrayToHexString(scriptSHA1);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String byteArrayToHexString(byte[] b) {
        StringBuilder result = new StringBuilder();
        for (byte value : b) {
            result.append(Integer.toString((value & 0xff) + 0x100, 16).substring(1));
        }
        return result.toString();
    }
}
