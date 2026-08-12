package com.github.fppt.jedismock.exception;

/**
 * Raised for a {@code NOSCRIPT}-prefixed error reply.
 *
 * <p>Deliberately a subtype of {@link ErrorReplyException}: callers that handle
 * error replies in bulk (such as the multi-bulk element loop in the Lua bridge)
 * must catch this one too.
 */
public class NoScriptException extends ErrorReplyException {

    public NoScriptException(String message) {
        super(message);
    }
}
