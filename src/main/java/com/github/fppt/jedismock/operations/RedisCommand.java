package com.github.fppt.jedismock.operations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Registers a {@link RedisOperation} under a command name. The members mirror
 * what real Redis reports for the command in {@code COMMAND INFO}, and
 * {@code CommandParityTest} checks them against a real server.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RedisCommand {
    String value();

    /**
     * Whether the command is queued when issued inside {@code MULTI}. Commands
     * that act on the connection or the transaction itself are not.
     */
    boolean transactional() default true;

    /**
     * The command's arity in Redis' own terms: the count includes the command
     * name, a positive value is exact and a negative value is a minimum (so
     * {@code -2} means "at least one argument"). {@code -1} accepts anything.
     */
    int arity() default -1;

    /**
     * {@code false} for commands that carry Redis' {@code write} flag, which
     * read-only scripts and the {@code _RO} command variants must reject.
     */
    boolean readOnly() default true;
}
