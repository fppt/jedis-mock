package com.github.fppt.jedismock.operations.scripting.cjson;

import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.TwoArgFunction;

import java.util.HashMap;
import java.util.Map;

public class LuaCjsonLib extends TwoArgFunction {

    @Override
    public LuaValue call(LuaValue modname, LuaValue env) {
        Map<LuaValue, LuaValue> cjsonMap = new HashMap<>();
        cjsonMap.put(LuaValue.valueOf("encode"), new Encode());
        cjsonMap.put(LuaValue.valueOf("decode"), new Decode());
        return new ImmutableLuaTable(cjsonMap);
    }
}
