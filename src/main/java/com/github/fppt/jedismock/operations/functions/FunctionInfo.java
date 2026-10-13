package com.github.fppt.jedismock.operations.functions;

import org.luaj.vm2.LuaClosure;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public class FunctionInfo {
    private final String description;
    private final LuaClosure function;
    private final String libraryName;
    private final Set<FunctionFlag> flags;

    public FunctionInfo(String description, LuaClosure function, String libraryName) {
        this(description, function, libraryName, EnumSet.noneOf(FunctionFlag.class));
    }

    public FunctionInfo(String description, LuaClosure function, String libraryName, Set<FunctionFlag> flags) {
        this.description = description;
        this.function = function;
        this.libraryName = libraryName;
        this.flags = flags.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(EnumSet.copyOf(flags));
    }

    public String getDescription() {
        return description;
    }

    public LuaClosure getFunction() {
        return function;
    }

    public String getLibraryName() {
        return libraryName;
    }

    /**
     * @return the flags the function was registered with, in {@link FunctionFlag}
     * declaration order; never {@code null}
     */
    public Set<FunctionFlag> getFlags() {
        return flags;
    }
}
