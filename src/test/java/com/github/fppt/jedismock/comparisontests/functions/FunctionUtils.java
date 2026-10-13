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

    /**
     * A library whose single function carries the {@code no-writes} flag, laid
     * out so that the function body is on line 3 of the library code.
     */
    static String getNoWritesFunctionLibraryCode(String engine, String libraryName, String functionName, String functionBody) {
        return "#!%s name=%s\nredis.register_function{function_name='%s', callback=function(KEYS, ARGV)\n %s \nend, flags={'no-writes'}}\n"
                .formatted(engine, libraryName, functionName, functionBody);
    }
}
