package com.github.fppt.jedismock.comparisontests;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ComparisonBase implements TestTemplateInvocationContextProvider,
        BeforeAllCallback, AfterAllCallback {
    private static RedisServer fakeServer;

    private static GenericContainer redis = new GenericContainer<>("redis:6.2-alpine")
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
                        new Jedis(fakeServer.getHost(), fakeServer.getBindPort(), 1000000),
                        new HostAndPort(fakeServer.getHost(), fakeServer.getBindPort())),
                new JedisTestTemplateInvocationContext("real",
                        new Jedis(redis.getContainerIpAddress(), redis.getFirstMappedPort()),
                        new HostAndPort(redis.getContainerIpAddress(), redis.getFirstMappedPort())));
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
                jedis.resetState();
                jedis.quit();
                jedis.close();
            });
        }
    }

}
