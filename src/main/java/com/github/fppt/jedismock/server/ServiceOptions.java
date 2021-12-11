package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.operations.server.MockExecutor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Xiaolu on 2015/4/22.
 */
public class ServiceOptions {
    private final RedisCommandInterceptor commandInterceptor;

    private ServiceOptions(
            RedisCommandInterceptor commandInterceptor) {
        this.commandInterceptor = commandInterceptor;

    }

    public RedisCommandInterceptor getCommandInterceptor() {
        return commandInterceptor;
    }

    public static ServiceOptions defaultOptions() {
        return new ServiceOptions(MockExecutor::proceed);
    }

    /**
     * A special type of interceptor which mocks only given number of command invocation and then breaks the connection.
     * @param n a number of commands to execute before the connection break.
     */
    public static ServiceOptions executeOnly(int n) {
        AtomicInteger count = new AtomicInteger();
        return new ServiceOptions((state, name, params) -> {
            if (count.incrementAndGet() > n) {
                return MockExecutor.breakConnection(state);
            } else {
                return MockExecutor.proceed(state, name, params);
            }
        });
    }

    /**
     * Set interceptor which handles all the operations sent to JedisMock and can be used for
     * overriding the standard behaviour.
     *
     * @param commandInterceptor - function which takes execution state, operation name and parameters
     *                          and overrides behavior of RedisOperationExecutor.
     */
    public static ServiceOptions withInterceptor(RedisCommandInterceptor commandInterceptor) {
        return new ServiceOptions(commandInterceptor);
    }
}
