package com.github.fppt.jedismock.comparisontests.functions;

class FunctionUtils {

    private FunctionUtils() {
    }

    static String getSingleFunctionLibraryCode(String engine, String libraryName, String functionName, String functionBody) {
        return "#!%s name=%s\n%s".formatted(engine, libraryName, getFunctionCode(functionName, functionBody));
    }

    static String getFunctionCode(String functionName, String functionBody) {
        return "redis.register_function('%s', function(KEYS, ARGV)\n %s \nend)\n".formatted(functionName, functionBody);
    }
}
