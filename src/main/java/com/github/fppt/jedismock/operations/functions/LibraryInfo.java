package com.github.fppt.jedismock.operations.functions;

import org.luaj.vm2.UpValue;

import java.util.Set;

public class LibraryInfo {
    private final Set<String> functionNames;
    private final String code;
    /**
     * the library's top-level chunk's {@code _ENV} upvalue,
     * shared by every closure defined within that chunk
     * (registered functions and any private helper functions
     * they call). {@code FCALL} mutates this single box to
     * switch the whole library over to the full API {@code
     * redis} table for the duration of the call, so helper
     * functions see the change without needing their own
     * upvalues patched individually.
     */
    private final UpValue environment;

    public LibraryInfo(Set<String> functionNames, String code, UpValue environment) {
        this.functionNames = functionNames;
        this.code = code;
        this.environment = environment;
    }

    public Set<String> getFunctionNames() {
        return functionNames;
    }

    public String getCode() {
        return code;
    }

    public UpValue getEnvironment() {
        return environment;
    }
}
