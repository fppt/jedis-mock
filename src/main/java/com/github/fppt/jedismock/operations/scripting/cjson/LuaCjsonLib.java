package com.github.fppt.jedismock.operations.scripting.cjson;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.TwoArgFunction;

public class LuaCjsonLib extends TwoArgFunction {

    @Override
    public LuaValue call(LuaValue modname, LuaValue env) {
        LuaTable cjson = new LuaTable();
        cjson.set("encode", new Encode());
        cjson.set("decode", new Decode());
        env.set("cjson", cjson);
        env.get("package").get("loaded").set("cjson", cjson);
        return cjson;
    }
}
