package com.github.fppt.jedismock.exception;

public class WrongStreamKeyException extends Exception {
    public WrongStreamKeyException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return java.util.Objects.requireNonNull(super.getMessage());
    }
}
