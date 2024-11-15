package com.github.fppt.jedismock.operations.scripting.cjson;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.util.Map;

class ImmutableLuaTable extends LuaTable {

    private static final String ATTEMPT_TO_MODIFY_A_READONLY_TABLE = "Attempt to modify a readonly table";

    ImmutableLuaTable(Map<LuaValue, LuaValue> entries) {
        super();
        entries.forEach(super::hashset);
    }

    @Override
    public void set(String key, LuaValue value) {
        throw new UnsupportedOperationException(ATTEMPT_TO_MODIFY_A_READONLY_TABLE);
    }

    @Override
    public void set(int key, LuaValue value) {
        throw new UnsupportedOperationException(ATTEMPT_TO_MODIFY_A_READONLY_TABLE);
    }

    @Override
    public void set(LuaValue key, LuaValue value) {
        throw new UnsupportedOperationException(ATTEMPT_TO_MODIFY_A_READONLY_TABLE);
    }

    @Override
    public void hashset(LuaValue key, LuaValue value) {
        throw new UnsupportedOperationException(ATTEMPT_TO_MODIFY_A_READONLY_TABLE);
    }

    @Override
    public void rawset(LuaValue key, LuaValue value) {
        throw new UnsupportedOperationException(ATTEMPT_TO_MODIFY_A_READONLY_TABLE);
    }

    @Override
    public void rawset(int key, LuaValue value) {
        throw new UnsupportedOperationException(ATTEMPT_TO_MODIFY_A_READONLY_TABLE);
    }
}
