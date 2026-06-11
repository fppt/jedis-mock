package com.github.fppt.jedismock.operations.keys.paramsparser;

public class ExpirationParamsException extends Exception {
    public ExpirationParamsException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return java.util.Objects.requireNonNull(super.getMessage());
    }
}
