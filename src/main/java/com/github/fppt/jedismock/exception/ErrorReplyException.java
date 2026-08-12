package com.github.fppt.jedismock.exception;

/**
 * Raised inside the Lua {@code redis.call} bridge when a command produced a
 * RESP error reply, so that the script can be aborted with the command's own
 * message.
 */
public class ErrorReplyException extends RuntimeException {

    public ErrorReplyException(String message) {
        super(message);
    }
}
