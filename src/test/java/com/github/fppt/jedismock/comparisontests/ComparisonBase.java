package com.github.fppt.jedismock.comparisontests;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class ComparisonBase implements TestTemplateInvocationContextProvider,
        BeforeAllCallback, AfterAllCallback {
    private static RedisServer fakeServer;

    private static GenericContainer redis = new GenericContainer<>("redis:5.0-alpine")
            .withExposedPorts(6379);


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        // Docker container:
        redis.start();

        //Start up the fake redis server
        fakeServer = RedisServer.newRedisServer(redis.getFirstMappedPort() + 1);
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
                        new Jedis(fakeServer.getHost(), fakeServer.getBindPort(), 1000000)),
                new JedisTestTemplateInvocationContext("real",
                        new Jedis(redis.getContainerIpAddress(), redis.getFirstMappedPort())));
    }

    private class JedisTestTemplateInvocationContext implements TestTemplateInvocationContext {

        private final String displayName;
        private final Jedis jedis;

        private JedisTestTemplateInvocationContext(String displayName, Jedis jedis) {
            this.displayName = displayName;
            this.jedis = jedis;
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
                    return parameterContext.getParameter().getType() == Jedis.class;
                }

                @Override
                public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
                    return jedis;
                }
            }, (AfterEachCallback) context ->
            {
                jedis.resetState();
                jedis.quit();
                jedis.close();
            });
        }
    }

}
