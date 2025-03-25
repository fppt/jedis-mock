package com.github.fppt.jedismock.comparisontests;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ComparisonBase implements TestTemplateInvocationContextProvider,
        BeforeAllCallback, AfterAllCallback {
    private static RedisServer fakeServer;

    private static final GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        // Docker container:
        redis.start();

        //Start up the fake redis server
        fakeServer = RedisServer.newRedisServer();
        fakeServer.start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        // Docker container:
        redis.stop();

        //Kill the fake redis server
        fakeServer.stop();
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return Stream.of(
                new JedisTestTemplateInvocationContext("mock",
                        new Jedis(fakeServer.getHost(), fakeServer.getBindPort(), 1000000),
                        new HostAndPort(fakeServer.getHost(), fakeServer.getBindPort())),
                new JedisTestTemplateInvocationContext("real",
                        new Jedis(redis.getHost(), redis.getFirstMappedPort()),
                        new HostAndPort(redis.getHost(), redis.getFirstMappedPort())));
    }

    private static class JedisTestTemplateInvocationContext implements TestTemplateInvocationContext {

        private final String displayName;
        private final Jedis jedis;
        private final HostAndPort hostAndPort;

        private JedisTestTemplateInvocationContext(String displayName, Jedis jedis, HostAndPort hostAndPort) {
            this.displayName = displayName;
            this.jedis = jedis;
            this.hostAndPort = hostAndPort;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return displayName;
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(new ParameterResolver() {
                @Override
                public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
                    return parameterContext.getParameter().getType() == Jedis.class
                            || parameterContext.getParameter().getType() == HostAndPort.class;
                }

                @Override
                public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
                    return parameterContext.getParameter().getType() == Jedis.class ? jedis : hostAndPort;
                }
            }, (AfterEachCallback) context ->
            {
                if (context.getExecutionException().isPresent() &&
                        context.getExecutionException().get().getMessage().startsWith(TestErrorMessages.DEADLOCK_ERROR_MESSAGE)) {
                    jedis.disconnect();
                    jedis.close();
                    return;
                }
                jedis.resetState();
                jedis.disconnect();
                jedis.close();
            });
        }
    }

}
