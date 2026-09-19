package com.github.fppt.jedismock.operations.functions;

import org.luaj.vm2.LuaClosure;

public class FunctionInfo {
    private final String description;
    private final LuaClosure function;
    private final String libraryName;

    public FunctionInfo(String description, LuaClosure function, String libraryName) {
        this.description = description;
        this.function = function;
        this.libraryName = libraryName;
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
}
