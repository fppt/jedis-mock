[![GitHub release](https://img.shields.io/github/release/fppt/jedis-mock.svg)](https://github.com/fppt/jedis-mock/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock)
[![Actions Status: build](https://github.com/fppt/jedis-mock/workflows/build/badge.svg)](https://github.com/fppt/jedis-mock/actions?query=workflow%3A"build") 

# Jedis-Mock

Jedis-Mock is a simple in-memory mock of Redis for Java testing, which can also work as test proxy. 
Despite its name, it works on network protocol level and can be used with any Redis client 
(be it [Jedis](https://github.com/redis/jedis), [Lettuce](https://github.com/lettuce-io/lettuce-core), [Redisson](https://github.com/redisson/redisson) or others).

When used as a mock, it allows you to test behaviour dependent on Redis without having to deploy an instance of Redis.

[List of currently supported Redis operations](supported_operations.md).

## Why, if we have TestContainers?
[TestContainers](https://www.testcontainers.org/) is a great solution for integration tests with real services, including Redis. However, sometimes we want to use mock or proxy for some of the tests for the following reasons:

* TestContainers require Docker. Jedis-Mock is just a Maven dependency which, when used as 'pure' mock, can be run on any machine, right now.
* TestContainers tests can be slow and resource-consuming. Jedis-Mock tests are lightning fast, which
encourages developers to write more tests and run them more often.
* Redis running in TestContainers is a "black box". We cannot verify what was actually called. 
  We cannot interfere with the reply. All this we can do with testing mock/proxy.
* We can use cluster connection APIs (e. g. `JedisCluster`) without spinning up 3 instances of Redis.
* If you wish, you can use Jedis-Mock *together* with TestContainers, delegating command execution 
  to a real Redis instance, intercepting some of the calls when needed.

## How can I ensure that this mock functions exactly like real Redis?
We employ two practices to achieve maximum compatibility with Redis:

1. Comparison testing: All tests for Jedis-Mock are executed twice—once against the mock and once against a real Redis instance running in TestContainers. This approach ensures that our tests for Jedis-Mock include accurate assertions.

2. Execution of native Redis tests: We continuously expand the suite of native Redis tests that are successfully executed against Jedis-Mock. These tests are the ones employed for regression testing of Redis itself. You can explore the specific tests being executed [here](.github/workflows/native-tests.yml).

However, the primary objective of a test mock is not to be a bug-to-bug compatible reimplementation, but to expose errors in the code being tested. Therefore, it is acceptable for a mock to fail more frequently than a real system and be more restrictive.

## Quickstart 

Add it as a dependency in Maven as:

```xml
<dependency>
  <groupId>com.github.fppt</groupId>
  <artifactId>jedis-mock</artifactId>
  <version>1.0.11</version>
</dependency>
```

Create a Redis server and bind it to your client:

```java
//This binds mock redis server to a random port
RedisServer server = RedisServer
        .newRedisServer()
        .start();

//Jedis connection:
Jedis jedis = new Jedis(server.getHost(), server.getBindPort());

//Lettuce connection:
RedisClient redisClient = RedisClient
        .create(String.format("redis://%s:%s",
        server.getHost(), server.getBindPort()));

//Redisson connection:
Config config = new Config();
config.useSingleServer().setAddress(
        String.format("redis://%s:%d",
        redisServer.getHost(), redisServer.getBindPort()));
RedissonClient client = Redisson.create(config);
```

From here test as needed.

## Cluster mode support

Sometimes you need to use cluster connection APIs in your tests. Jedis-Mock can emulate "cluster mode" by mocking a single node holding all the hash slots (0-16383) so that common connectivity libraries can successfully connect and work. Just use `withClusterModeEnabled()` for `ServiceOptions`:

```java
server = RedisServer
        .newRedisServer()
        .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
        .start();

//JedisCluster connection:
Set<HostAndPort> jedisClusterNodes = new HashSet<>();
jedisClusterNodes.add(new HostAndPort(server.getHost(), server.getBindPort()));
JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes);


//Lettuce connection:
RedisClusterClient redisClient = RedisClusterClient
        .create(String.format("redis://%s:%s", server.getHost(), server.getBindPort()));
```

Note that support of `CLUSTER` subcommands is limited to the  minimum that is necessary for successful usage of `JedisCluster`/`RedisClusterClient`.

## Using `RedisCommandInterceptor`

`RedisCommandInterceptor` is a functional interface which can be used to intercept calls to Jedis-Mock. 
You can use it as following:

```java
RedisServer server = RedisServer
    .newRedisServer()
    .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
        if ("get".equalsIgnoreCase(roName)) {
            //You can can imitate any reply from Redis
            return Response.bulkString(Slice.create("MOCK_VALUE"));
        } else if ("echo".equalsIgnoreCase(roName)) {
            //You can write any verifications here
            assertEquals("hello", params.get(0).toString());
            //And imitate connection breaking
            return MockExecutor.breakConnection(state);
        } else {
            //Delegate execution to JedisMock which will mock the real Redis behaviour (when it can)
            return MockExecutor.proceed(state, roName, params);
        }
        //NB: you can also delegate to a 'real' Redis in TestContainers here
    }))
    .start();
try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
    assertEquals("MOCK_VALUE", jedis.get("foo"));
    assertEquals("OK", jedis.set("bar", "baz"));
    assertThrows(JedisConnectionException.class, () -> jedis.echo("hello"));
}
server.stop();
```

:warning: if you are going to mutate the shared state, synchronize on `state.lock()` first!
(See how it's done in [`MockExecutor#proceed`](src/main/java/com/github/fppt/jedismock/operations/server/MockExecutor.java#L23)). 

## Fault tolerance testing

We can make a RedisServer close connection after several commands. This will cause a connection exception for clients.

```java
RedisServer server = RedisServer
                .newRedisServer()
                 //This is a special type of interceptor
                .setOptions(ServiceOptions.executeOnly(3))
                .start();
try (Jedis jedis = new Jedis(server.getHost(),
        server.getBindPort())) {
    assertEquals(jedis.set("ab", "cd"), "OK");
    assertEquals(jedis.set("ab", "cd"), "OK");
    assertEquals(jedis.set("ab", "cd"), "OK");
    assertThrows(JedisConnectionException.class, () -> jedis.set("ab", "cd"));
}
```

## Lua scripting support

JedisMock supports Lua scripting (`EVAL`, `EVALSHA`, `SCRIPT LOAD/EXISTS/FLUSH` commands) via [luaj](https://github.com/luaj/luaj).  

```java
String script =
                "local a, b = 0, 1\n" +
                "for i = 2, ARGV[1] do\n" +
                "  local temp = a + b\n" +
                "  a = b\n" +
                "  b = temp\n" +
                "  redis.call('RPUSH',KEYS[1], temp)\n" +
                "end\n" ;
jedis.eval(script, 1, "mylist", "10");
//Yields first 10 Fibonacci numbers
jedis.lrange("mylist", 0, -1));        
```

:warning: Lua language capabilities are restricted to what is provided by current LuaJ version. Methods provided by `redis` global object are currently restricted to what was available in Redis version 2.6.0 (see [redis.lua](src/main/resources/redis.lua)). 

Feel free to report an issue if you have any problems with Lua scripting in Jedis-Mock.

## Supported and Missing Operations

All currently supported and missing operations are listed [here](supported_operations.md).

If you get the following error:

```
Unsupported operation {}
```

please feel free to create an issue requesting the missing operation, 
or implement it yourself in interceptor and send us the code. It's fun!

