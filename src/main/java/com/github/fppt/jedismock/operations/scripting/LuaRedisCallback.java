package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.operations.connection.Select;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import redis.clients.jedis.util.RedisInputStream;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.operations.scripting.Eval.embedLuaListToValue;

public class LuaRedisCallback {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LuaRedisCallback.class);
    private static final String NOSCRIPT_PREFIX = "NOSCRIPT ";

    private final OperationExecutorState state;

    public LuaRedisCallback(final OperationExecutorState state) {
        this.state = state;
    }

    public LuaValue call(LuaValue args) {
        String operationName = args.get(1).tojstring();
        List<Slice> a = new ArrayList<>();
        for (int i = 2; i <= args.length(); i++) {
            LuaValue arg = args.get(i);
            if (arg instanceof LuaString) {
                a.add(Slice.create(((LuaString) arg).m_bytes));
            } else {
                a.add(Slice.create(args.get(i).tojstring()));
            }
        }
        return execute(operationName, a);
    }

    public LuaValue pcall(LuaValue args) {
        try {
            return call(args);
        } catch (final Exception e) {
            LuaTable errorTable = new LuaTable();
            errorTable.set(LuaValue.valueOf("err"), LuaValue.valueOf(e.getMessage()));
            return errorTable;
        }
    }

    public String sha1hex(String x) {
        return Script.getScriptSHA(x);
    }

    public void log(int level, String message) {
        LOG.info("redis.log ({}, {})", level, message);
    }

    private LuaValue execute(final String operationName, final List<Slice> args) {

        final RedisOperation operation =
                //Specific support for SELECT,
                //see https://redis.io/docs/manual/programmability/lua-api/#using-selectcommandsselect-inside-scripts
                "select".equalsIgnoreCase(operationName) ? new Select(state, args) :
                        CommandFactory.buildOperation(operationName.toLowerCase(), true, state, args);
        if (operation != null) {
            throwOnUnsupported(operation);
            Slice result = operation.execute();
            if (Response.NULL.equals(result)) {
                return LuaValue.FALSE;
            } else {
                byte[] data = result.data();
                return toLuaValue(new RedisInputStream(new ByteArrayInputStream(data)));
            }
        }
        throw new RuntimeException("Operation not implemented!");
    }

    private static void throwOnUnsupported(RedisOperation operation) {
        if (operation.getClass().equals(Eval.class)) {
            throw new RuntimeException("This Redis command is not allowed from scripts");
        }
    }

    private static LuaValue toLuaValue(final RedisInputStream is) {
        byte b = is.readByte();
        switch (b) {
            case '+':
                return LuaValue.valueOf(processStatusCodeReply(is));
            case '$':
                return LuaValue.valueOf(processBulkReply(is));
            case '*':
                return embedLuaListToValue(processMultiBulkReply(is));
            case ':':
                return LuaValue.valueOf(processInteger(is));
            case '-':
                String message = is.readLine();
                if (message.startsWith(NOSCRIPT_PREFIX)) {
                    throw new JedisNoScriptException(message);
                } else {
                    throw new JedisDataException(message);
                }
            default:
                return LuaValue.NONE;
        }

    }

    private static byte[] processStatusCodeReply(RedisInputStream is) {
        return is.readLineBytes();
    }

    private static byte[] processBulkReply(RedisInputStream is) {
        int len = is.readIntCrLf();
        if (len <= 0) {
            return new byte[0];
        } else {
            byte[] read = new byte[len];
            int size;
            for (int offset = 0; offset < len; offset += size) {
                size = is.read(read, offset, len - offset);
                if (size == -1) {
                    throw new RuntimeException("It seems like server has closed the connection.");
                }
            }
            is.readByte();
            is.readByte();
            return read;
        }
    }

    private static Long processInteger(RedisInputStream is) {
        return is.readLongCrLf();
    }

    private static List<LuaValue> processMultiBulkReply(RedisInputStream is) {
        int num = is.readIntCrLf();
        if (num <= 0) {
            return Collections.emptyList();
        } else {
            List<LuaValue> ret = new ArrayList<>(num);
            for (int i = 0; i < num; ++i) {
                try {
                    ret.add(toLuaValue(is));
                } catch (JedisDataException e) {
                    System.err.println(e.getMessage());
                }
            }
            return ret;
        }
    }
}
