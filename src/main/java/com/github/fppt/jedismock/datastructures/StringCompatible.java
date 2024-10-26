package com.github.fppt.jedismock.datastructures;


public abstract class StringCompatible implements RMDataStructure {

    @Override
    public final String getTypeName() {
        return "string";
    }

}
