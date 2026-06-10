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
import java.util.Set;

import static com.github.fppt.jedismock.operations.scripting.Eval.embedLuaListToValue;

public class LuaRedisCallback {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LuaRedisCallback.class);
    private static final String NOSCRIPT_PREFIX = "NOSCRIPT ";
    private static final String WRONG_ARGS_FROM_SCRIPT =
            "Wrong number of args calling command from script";
    //Commands real Redis flags as not-callable from a script and which jedis-mock
    //does not (fully) model. Kept lower-case for case-insensitive lookup.
    private static final Set<String> SCRIPT_DISALLOWED_COMMANDS = Collections.singleton("cluster");

    private final OperationExecutorState state;

    public LuaRedisCallback(final OperationExecutorState state) {
        this.state = state;
    }

    public LuaValue call(LuaValue args) {
        if (args.length() < 1) {
            throw new IllegalStateException("Please specify at least one argument for this redis lib call");
        }
        String operationName = args.get(1).tojstring();
        List<Slice> a = new ArrayList<>();
        for (int i = 2; i <= args.length(); i++) {
            LuaValue arg = args.get(i);
            if (arg instanceof LuaString) {
                a.add(Slice.create(((LuaString) arg).m_bytes));
            } else if (arg.isnumber()) {
                a.add(Slice.create(arg.tojstring()));
            } else {
                //Reject tables/booleans/nil before dispatch, as real Redis does,
                //so the cached argv array is left intact for the next command.
                throw new IllegalStateException(
                        "Command arguments must be strings or integers");
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
        if (x == null) {
            //redis.sha1hex() with no argument: real Redis reports an arity error
            //instead of crashing (here, a NullPointerException down the stack).
            throw new IllegalStateException("wrong number of arguments to redis.sha1hex()");
        }
        return Script.getScriptSHA(x);
    }

    public void log(int level, String message) {
        LOG.info("redis.log ({}, {})", level, message);
    }

    private LuaValue execute(final String operationName, final List<Slice> args) {
        if (SCRIPT_DISALLOWED_COMMANDS.contains(operationName.toLowerCase())) {
            //Commands that exist in the server but are flagged no-script. They are
            //not (fully) modelled here, so reject them with the server's own
            //wording rather than the generic "Unknown command".
            throw new IllegalStateException("This command is not allowed from script");
        }

        final RedisOperation operation =
                //Specific support for SELECT,
                //see https://redis.io/docs/manual/programmability/lua-api/#using-selectcommandsselect-inside-scripts
                "select".equalsIgnoreCase(operationName) ? new Select(state, args) :
                        CommandFactory.buildOperation(operationName.toLowerCase(), true, state, args);
        if (operation == null) {
            throw new IllegalStateException("Unknown command called from script");
        }
        throwOnUnsupported(operation);
        final Slice result;
        try {
            result = operation.execute();
        } catch (IllegalArgumentException e) {
            //Some commands signal an arity mismatch by throwing (e.g. INCR with no
            //key trips an IndexOutOfBounds, wrapped here) rather than returning an
            //error reply; normalise it like the returned-error case below.
            if (isArityError(e.getMessage())) {
                throw new IllegalStateException(WRONG_ARGS_FROM_SCRIPT);
            }
            throw e;
        }
        if (result.toString().startsWith("-") && isArityError(result.toString())) {
            //Real Redis rejects an arity mismatch before dispatch with this
            //script-specific message; translate the command's own arity error.
            throw new IllegalStateException(WRONG_ARGS_FROM_SCRIPT);
        }
        if (Response.NULL.equals(result)) {
            return LuaValue.FALSE;
        } else {
            byte[] data = result.data();
            return toLuaValue(new RedisInputStream(new ByteArrayInputStream(data)));
        }
    }

    private static boolean isArityError(String message) {
        return message != null && message.contains("wrong number of arguments");
    }

    private static void throwOnUnsupported(RedisOperation operation) {
        if (operation.getClass().equals(Eval.class)) {
            throw new IllegalStateException("This command is not allowed from scripts");
        }
    }

    private static LuaValue toLuaValue(final RedisInputStream is) {
        byte b = is.readByte();
        switch (b) {
            case '+':
                //A status reply ("+OK") converts to a Lua table {ok="OK"}, not a
                //bare string — matching real Redis, where scripts read
                //redis.call('set', ...)['ok'].
                LuaTable statusTable = new LuaTable();
                statusTable.set(LuaValue.valueOf("ok"), LuaValue.valueOf(processStatusCodeReply(is)));
                return statusTable;
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
                    throw new IllegalStateException("It seems like server has closed the connection.");
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
