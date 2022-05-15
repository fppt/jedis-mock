package com.github.fppt.jedismock.datastructures;

public interface RMDataStructure {
    void raiseTypeCastException();
    String getTypeName();

    default Slice getAsSlice() {
        raiseTypeCastException();
        return null;
    }
}
