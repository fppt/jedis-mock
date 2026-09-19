package com.github.fppt.jedismock.operations.functions;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.scripting.Eval;
import com.github.fppt.jedismock.operations.scripting.InterruptibleDebugLib;
import com.github.fppt.jedismock.operations.scripting.LuaSandbox;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.UpValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.parseQuotedString;

@RedisCommand("function")
public class Function extends AbstractRedisOperation {
    private static final Slice USAGE = Response.array(Stream.of(
            "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "LOAD [REPLACE] <FUNCTION CODE>",
            "    Create a new library with the given library name and code.",
            "DELETE <LIBRARY NAME>",
            "    Delete the given library.",
            "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
            "    Return general information on all the libraries:",
            "    * Library name",
            "    * The engine used to run the Library",
            "    * Functions list",
            "    * Library code (if WITHCODE is given)",
            "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
            "STATS",
            "    Return information about the current function running:",
            "    * Function name",
            "    * Command used to run the function",
            "    * Duration in MS that the function is running",
            "    If no function is running, return nil",
            "    In addition, returns a list of available engines.",
            "KILL",
            "    Kill the current running function.",
            "FLUSH [ASYNC|SYNC]",
            "    Delete all the libraries.",
            "    When called without the optional mode argument, the behavior is determined by the",
            "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the libraries.",
            "    * SYNC: Synchronously flush the libraries.",
            // TODO: RESTORE and DUMP.
            "HELP",
            "    Print this help."
    ).map(line -> Response.bulkString(Slice.create(line))).collect(Collectors.toList()));

    private final OperationExecutorState state;

    public Function(RedisBase base, List<Slice> params, OperationExecutorState state) {
        super(base, params);
        this.state = state;
    }

    @Override
    protected Slice response() {
        String subcommand = params().get(0).toString();
        switch (subcommand.toUpperCase()) {
            case "LOAD":
                return load();
            case "FLUSH":
                return flush();
            case "DELETE":
                return delete();
            case "LIST":
                return list();
            case "HELP":
                return help();
            default:
                throw new ArgumentException(String.format("ERR unknown subcommand '%s'. Try FUNCTION HELP.", subcommand));
        }
    }

    private Slice load() {
        LoadParams loadParams = parseLoadParams(params());
        LibraryMetadata metadata = parseLibraryMetadata(loadParams.script);
        LibraryLoadResult result = callLibraryScript(metadata.code, metadata.libraryName);
        base().registerLuaLibrary(metadata.libraryName, loadParams.script, result.sharedEnvironment, result.functions, loadParams.replace);
        return Response.bulkString(Slice.create(metadata.libraryName));
    }

    private Slice flush() {
        validateFlushParams(params());
        base().flushLuaLibraries();
        return Response.simpleString("OK");
    }

    private Slice delete() {
        base().deleteLuaLibrary(params().get(1).toString());
        return Response.simpleString("OK");
    }

    private Slice list() {
        boolean includeCode = false;
        String libraryNamePattern = null;
        for (int i = 1; i < params().size(); i++) {
            String option = params().get(i).toString();
            if ("WITHCODE".equalsIgnoreCase(option) && !includeCode) {
                includeCode = true;
            } else if ("LIBRARYNAME".equalsIgnoreCase(option) && libraryNamePattern == null) {
                if (params().size() <= i + 1) {
                    throw new ArgumentException("ERR library name argument was not given");
                }
                libraryNamePattern = params().get(++i).toString();
            } else {
                throw new ArgumentException("ERR Unknown argument " + option);
            }
        }
        Map<String, LibraryInfo> libraries = filterLibraries(base().getLuaLibraries(), libraryNamePattern);
        boolean finalIncludeCode = includeCode;
        return Response.array(libraries.entrySet().stream()
                .map((Map.Entry<String, LibraryInfo> library) -> buildLibraryInfo(library, finalIncludeCode))
                .collect(Collectors.toList()));
    }

    private Slice help() {
        return USAGE;
    }

    private static LoadParams parseLoadParams(List<Slice> params) {
        if (params.size() == 2) {
            return new LoadParams(false, params.get(1).toString());
        }
        if (params.size() == 3 && params.get(1).toString().equalsIgnoreCase("REPLACE")) {
            return new LoadParams(true, params.get(2).toString());
        }
        throw new ArgumentException(String.format("ERR Unknown option given: %s", params.get(1).toString()));
    }

    private static LibraryMetadata parseLibraryMetadata(String library) {
        if (!library.startsWith("#!")) {
            throw new ArgumentException("ERR Missing library metadata");
        }
        int shebangEnd = library.indexOf('\n');
        if (shebangEnd == -1) {
            throw new ArgumentException("ERR Invalid library metadata");
        }
        String[] shebangParts = library.substring(2, shebangEnd).split(" ");
        String engine = shebangParts[0];
        if (!"LUA".equalsIgnoreCase(engine)) {
            throw new ArgumentException(String.format("ERR Engine '%s' not found", engine));
        }
        String libraryName = parseLibraryName(shebangParts);
        if (isInvalidLibraryOrFunctionName(libraryName)) {
            throw new ArgumentException("ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long");
        }
        String code = library.substring(shebangEnd);
        return new LibraryMetadata(libraryName, code);
    }

    private static String parseLibraryName(String[] shebangParts) {
        String libraryName = null;
        for (int i = 1; i < shebangParts.length; i++) {
            String part = shebangParts[i];
            if (part.startsWith("name=")) {
                if (libraryName != null) {
                    throw new ArgumentException("ERR Invalid metadata value, name argument was given multiple times");
                }
                libraryName = parseQuotedString(part.substring("name=".length()));
                if (libraryName == null) {
                    throw new ArgumentException("ERR Invalid library metadata");
                }
            } else {
                throw new ArgumentException("ERR Invalid metadata value given: " + part);
            }
        }
        if (libraryName == null) {
            throw new ArgumentException("ERR Library name was not given");
        }
        return libraryName;
    }

    private static void validateFlushParams(List<Slice> params) {
        if (params.size() > 2) {
            throw new ArgumentException(String.format("ERR unknown subcommand or wrong number of arguments for '%s'. Try FUNCTION HELP.", params.get(0).toString()));
        } else if (params.size() == 2) {
            String mode = params.get(1).toString();
            if (!"SYNC".equalsIgnoreCase(mode) && !"ASYNC".equalsIgnoreCase(mode)) {
                throw new ArgumentException("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
            }
        }
    }

    private LibraryLoadResult callLibraryScript(String code, String libraryName) {
        Globals globals = new Globals();
        globals.load(new PackageLib());
        LuaC.install(globals);
        LuaValue redisObject = LuaValue.tableOf();
        RegisterFunction registerFunction = new RegisterFunction(libraryName);
        redisObject.set("register_function", registerFunction);
        globals.set("redis", redisObject);
        LibraryLoadDebugLib debugLib = new LibraryLoadDebugLib(state.scriptingManager(), 500);
        globals.load(debugLib);
        //During library load only register_function (and friends) is available on
        //`redis` (redis.call is not, since executing commands here would run at
        //LOAD time rather than per-invocation); guard missing keys on it so that,
        //like real Redis, a script attempting redis.call(...) here fails with
        //"nonexistent global variable" rather than a plain nil-call error.
        LuaTable sandbox = LuaSandbox.installForLibraryLoad(globals);

        LuaValue script;
        try {
            script = globals.load(code, "@user_function");
        } catch (LuaError e) {
            throw new ArgumentException(String.format("ERR Error compiling function: %s", e.getMessage()));
        }

        //The chunk's own _ENV upvalue is shared by every closure defined within it
        //(registered functions and any private helper functions they call, since
        //they're all nested in this one chunk). FCALL later mutates this same box
        //to switch the whole library over to the full API `redis` table, so helper
        //functions pick up the change without needing their own upvalues patched.
        UpValue sharedEnvironment = null;
        if (script instanceof LuaClosure) {
            LuaClosure closure = (LuaClosure) script;
            if (closure.upValues.length > 0) {
                sharedEnvironment = closure.upValues[0];
                sharedEnvironment.setValue(sandbox);
            }
        }

        debugLib.start();
        try {
            script.call();
        } catch (LuaError e) {
            String message = Eval.libraryLoadErrorMessage(e);
            throw new ArgumentException("ERR Error registering functions: " + message);
        }
        registerFunction.completed = true;
        debugLib.stop();
        if (registerFunction.functions.isEmpty()) {
            throw new ArgumentException("ERR No functions registered");
        }
        return new LibraryLoadResult(registerFunction.functions, sharedEnvironment);
    }

    private static boolean isInvalidLibraryOrFunctionName(String libraryName) {
        return libraryName.isEmpty() ||
                !libraryName.chars().allMatch(c -> c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_');
    }

    private Map<String, LibraryInfo> filterLibraries(Map<String, LibraryInfo> libraries, String libraryNamePattern) {
        if (libraryNamePattern == null) {
            return libraries;
        }
        Pattern pattern = Pattern.compile(Utils.createRegexFromGlob(libraryNamePattern));
        return libraries.entrySet().stream()
                .filter(entry -> pattern.matcher(entry.getKey()).matches())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Slice buildLibraryInfo(Map.Entry<String, LibraryInfo> library, boolean includeCode) {
        List<Slice> items = new ArrayList<>();
        items.add(Response.bulkString(Slice.create("library_name")));
        items.add(Response.bulkString(Slice.create(library.getKey())));
        items.add(Response.bulkString(Slice.create("engine")));
        items.add(Response.bulkString(Slice.create("LUA")));
        items.add(Response.bulkString(Slice.create("functions")));
        items.add(Response.array(library.getValue().getFunctionNames().stream().map(functionName -> {
            FunctionInfo functionInfo = base().getLuaFunctionInfo(functionName);
            return Response.array(
                    Response.bulkString(Slice.create("name")),
                    Response.bulkString(Slice.create(functionName)),
                    Response.bulkString(Slice.create("description")),
                    functionInfo.getDescription() != null ?
                            Response.bulkString(Slice.create(functionInfo.getDescription())) :
                            Response.NULL,
                    Response.bulkString(Slice.create("flags")),
                    Response.array()
            );
        }).collect(Collectors.toList())));
        if (includeCode) {
            items.add(Response.bulkString(Slice.create("library_code")));
            items.add(Response.bulkString(Slice.create(library.getValue().getCode())));
        }
        return Response.array(items);
    }

    private static class LoadParams {
        final boolean replace;
        final String script;

        private LoadParams(boolean replace, String script) {
            this.replace = replace;
            this.script = script;
        }
    }

    private static class LibraryMetadata {
        final String libraryName;
        final String code;

        private LibraryMetadata(String libraryName, String code) {
            this.libraryName = libraryName;
            this.code = code;
        }
    }

    private static class LibraryLoadResult {
        final Map<String, FunctionInfo> functions;
        final UpValue sharedEnvironment;

        private LibraryLoadResult(Map<String, FunctionInfo> functions, UpValue sharedEnvironment) {
            this.functions = functions;
            this.sharedEnvironment = sharedEnvironment;
        }
    }

    private static class RegisterFunction extends VarArgFunction {
        private final String libraryName;
        private final Map<String, FunctionInfo> functions = new HashMap<>();
        private boolean completed = false;

        private RegisterFunction(String libraryName) {
            this.libraryName = libraryName;
        }

        @Override
        public Varargs invoke(Varargs varargs) {
            throw new InvalidRegistrationException("ERR wrong number of arguments to redis.register_function");
        }

        @Override
        public LuaValue call(LuaValue arg) {
            if (!arg.istable()) {
                throw new InvalidRegistrationException("ERR calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).");
            }
            LuaTable table = arg.checktable();

            String functionName = null;
            String description = null;
            LuaClosure callback = null;

            LuaValue key = LuaValue.NIL;
            Varargs entry;
            while ((entry = table.next(key)) != NIL) {
                key = entry.arg1();
                if (!key.isstring()) {
                    throw new InvalidRegistrationException("ERR named argument key given to redis.register_function is not a string");
                }
                LuaValue value = entry.arg(2);
                switch (key.checkjstring()) {
                    case "function_name":
                        if (!value.isstring()) {
                            throw new InvalidRegistrationException("ERR function_name argument given to redis.register_function must be a string");
                        }
                        functionName = value.checkjstring();
                        break;
                    case "description":
                        if (!value.isstring()) {
                            throw new InvalidRegistrationException("ERR description argument given to redis.register_function must be a string");
                        }
                        description = value.checkjstring();
                        break;
                    case "callback":
                        if (!value.isclosure()) {
                            throw new InvalidRegistrationException("ERR callback argument given to redis.register_function must be a function");
                        }
                        callback = value.checkclosure();
                        break;
                    case "flags":
                        if (!value.istable()) {
                            throw new InvalidRegistrationException("ERR flags argument to redis.register_function must be a table representing function flags");
                        }
                        if (value.length() != 0) {
                            // No flags are supported yet.
                            throw new InvalidRegistrationException("ERR unknown flag given");
                        }
                        break;
                    default:
                        throw new InvalidRegistrationException("ERR unknown argument given to redis.register_function");
                }
            }

            if (functionName == null) {
                throw new InvalidRegistrationException("ERR redis.register_function must get a function name argument");
            }
            if (callback == null) {
                throw new InvalidRegistrationException("ERR redis.register_function must get a callback argument");
            }

            return register(functionName, new FunctionInfo(description, callback, libraryName));
        }

        @Override
        public LuaValue call(LuaValue functionName, LuaValue function) {
            if (!functionName.isstring()) {
                throw new InvalidRegistrationException("ERR first argument to redis.register_function must be a string");
            }
            if (!function.isclosure()) {
                throw new InvalidRegistrationException("ERR second argument to redis.register_function must be a function");
            }
            return register(functionName.checkjstring(), new FunctionInfo(null, function.checkclosure(), libraryName));
        }

        private LuaValue register(String functionName, FunctionInfo functionInfo) {
            if (completed) {
                throw new InvalidRegistrationException("ERR redis.register_function can only be called on FUNCTION LOAD command");
            }
            if (isInvalidLibraryOrFunctionName(functionName)) {
                throw new InvalidRegistrationException("ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long");
            }
            if (functions.containsKey(functionName)) {
                throw new InvalidRegistrationException("ERR Function already exists in the library");
            }
            functions.put(functionName, functionInfo);
            return LuaValue.NIL;
        }

        /**
         * {@link org.luaj.vm2.LuaValue} defines {@code equals} as identity
         * ({@code this == obj}) but no {@code hashCode}. Make both explicit and
         * mutually consistent (identity semantics) so SpotBugs is satisfied; each
         * debug-lib instance is a distinct object anyway.
         */
        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    private static class LibraryLoadDebugLib extends InterruptibleDebugLib {
        private long startNanos;
        private final long timeoutNanos;
        private boolean stopped = false;

        private LibraryLoadDebugLib(ScriptingManager scriptingManager, long timeoutMillis) {
            super(scriptingManager);
            this.timeoutNanos = timeoutMillis * 1_000_000;
        }

        void start() {
            startNanos = System.nanoTime();
        }

        void stop() {
            stopped = true;
        }

        @Override
        public void onInstruction(int pc, Varargs v, int top) {
            long elapsed = System.nanoTime() - startNanos;
            if (!stopped && elapsed > timeoutNanos) {
                throw new InvalidRegistrationException("ERR FUNCTION LOAD timeout");
            }
            super.onInstruction(pc, v, top);
        }

        /**
         * {@link org.luaj.vm2.LuaValue} defines {@code equals} as identity
         * ({@code this == obj}) but no {@code hashCode}. Make both explicit and
         * mutually consistent (identity semantics) so SpotBugs is satisfied; each
         * debug-lib instance is a distinct object anyway.
         */
        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }
    }

    private static class InvalidRegistrationException extends LuaError {
        public InvalidRegistrationException(String message) {
            super(LuaValue.tableOf(new LuaValue[]{LuaValue.valueOf("err"), LuaValue.valueOf(message)}));
        }
    }
}
