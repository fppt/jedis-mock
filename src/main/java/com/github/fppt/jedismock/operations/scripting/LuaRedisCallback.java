package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandDescriptor;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.operations.scripting.ScriptingUtils.embedLuaListToValue;

public class LuaRedisCallback {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LuaRedisCallback.class);
    private static final String NOSCRIPT_PREFIX = "NOSCRIPT ";
    private static final String WRONG_ARGS_FROM_SCRIPT =
            "Wrong number of args calling command from script";
    //Carries its error code already, so that redis.pcall() hands the script
    //exactly the reply real Redis gives: "ERR Write commands ...".
    private static final String READ_ONLY_WRITE_REJECTED =
            "ERR Write commands are not allowed from read-only scripts.";
    //Commands real Redis flags as not-callable from a script and which jedis-mock
    //does not (fully) model. Kept lower-case for case-insensitive lookup.
    private static final Set<String> SCRIPT_DISALLOWED_COMMANDS = buildDisallowedCommandsSet();

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
        } catch (final ScriptKilledError e) {
            //The kill is not a command error the script may handle: let it
            //propagate, or a script returning this pcall's result directly
            //(with no further instruction for the kill hook) would finish.
            throw e;
        } catch (final Exception e) {
            LuaTable errorTable = new LuaTable();
            //Some exceptions carry no message; String.valueOf avoids an NPE in valueOf.
            errorTable.set(LuaValue.valueOf("err"), LuaValue.valueOf(String.valueOf(e.getMessage())));
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

        final boolean select = "select".equalsIgnoreCase(operationName);
        final CommandDescriptor command = CommandFactory.lookup(operationName.toLowerCase());
        //Only data commands may be called from a script, plus SELECT.
        if (command == null || !(command.transactional() || select)) {
            throw new IllegalStateException("Unknown command called from script");
        }
        final Slice result;
        try {
            command.validateArity(args.size());
            if (!command.readOnly() && state.scriptingManager().isReadOnly()) {
                //A read-only run (EVAL_RO, EVALSHA_RO, FCALL_RO, or a no-writes
                //function) refuses the write here, once the command passed the
                //lookup and arity checks, in the same order as real Redis.
                throw new IllegalStateException(READ_ONLY_WRITE_REJECTED);
            }
            if (!command.readOnly() && !state.scriptingManager().markWrite()) {
                //A kill was granted before this write: abort now rather than
                //execute a write the killer was promised would not happen.
                //(When markWrite succeeds the script counts as having written,
                //whether or not the command succeeds: KILL will say UNKILLABLE.)
                throw InterruptibleDebugLib.killedError();
            }
            final RedisOperation operation =
                    //Specific support for SELECT,
                    //see https://redis.io/docs/manual/programmability/lua-api/#using-selectcommandsselect-inside-scripts
                    select ? new Select(state, args) : command.instantiate(state, args);
            result = operation.execute();
        } catch (IllegalArgumentException e) {
            //The arity check throws; so may a command that trips an
            //IndexOutOfBounds on a malformed call (wrapped by execute()).
            //Normalise both like the returned-error case below.
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
                    //An error reply nested inside a multi-bulk is dropped from the
                    //resulting Lua array (matching how scripts see partial errors).
                    LOG.warn("Skipping error element in multi-bulk reply: {}", e.getMessage());
                }
            }
            return ret;
        }
    }

    private static Set<String> buildDisallowedCommandsSet() {
        Set<String> commands = new HashSet<>();
        commands.add("cluster");
        commands.add("eval");
        commands.add("evalsha");
        commands.add("eval_ro");
        commands.add("evalsha_ro");
        commands.add("fcall");
        commands.add("fcall_ro");
        commands.add("function");
        return commands;
    }
}
