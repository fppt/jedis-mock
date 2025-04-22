[![GitHub release](https://img.shields.io/github/release/fppt/jedis-mock.svg)](https://github.com/fppt/jedis-mock/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock)
[![Actions Status: build](https://github.com/fppt/jedis-mock/workflows/build/badge.svg)](https://github.com/fppt/jedis-mock/actions?query=workflow%3A"build") 

# Jedis-Mock

Jedis-Mock is a simple in-memory mock of Redis/Valkey for Java testing, which can also work as test proxy. 
Despite its name, it works on network protocol level and can be used with any Redis client 
(be it [Jedis](https://github.com/redis/jedis), [Lettuce](https://github.com/lettuce-io/lettuce-core), [Redisson](https://github.com/redisson/redisson) or others).

When used as a mock, it allows you to test behaviour dependent on Redis without having to deploy an instance of Redis.

[List of currently supported Redis operations](supported_operations.md).

## Why, if we have TestContainers?
[TestContainers](https://www.testcontainers.org/) is a great solution for integration tests with real services, including Redis. However, sometimes we want to use mock or proxy for some of the tests for the following reasons:

* TestContainers require Docker. Jedis-Mock is just a Maven dependency which, when used as 'pure' mock, can be run on any machine, right now.
* TestContainers tests can be slow and resource-consuming. Jedis-Mock tests are lightning fast, which
encourages developers to write more tests and run them more often.
* While Redis running in TestContainers is a "black box", Jedis-Mock facilitates "white box" testing: we can verify what was actually called and interfere with the reply via a [command interceptor](#interceptor).
* With Jedis-Mock in [cluster emulation mode](#cluster), one can use cluster connection APIs (e. g. `JedisCluster`) without spinning up 3 instances of Redis.
* Using [clock injection](#clockinjection), we can "fast forward" and avoid waiting before keys expiration, or, on the contrary, "freeze time" and be guaranteed that keys are not expired before the end of the test scenario.   
* If you wish, you can use Jedis-Mock *together* with TestContainers, delegating command execution 
  to a real Redis instance, intercepting some of the calls when needed.

## How can I ensure that this mock functions exactly like real Redis?
We employ two practices to achieve maximum compatibility with Redis:

1. Comparison testing: All tests for Jedis-Mock are executed twice—once against the mock and once against a real Redis instance running in TestContainers. This approach ensures that our tests for Jedis-Mock include accurate assertions.

2. Execution of native Redis tests: We continuously expand the suite of native Redis tests that are successfully executed against Jedis-Mock. These tests are the ones employed for regression testing of Redis itself. You can explore the specific tests being executed [here](.github/workflows/native-tests.yml).

However, the primary objective of a test mock is not to be a bug-to-bug compatible reimplementation, but to expose errors in the code being tested. Therefore, it is acceptable for a mock to fail more frequently than a real system and be more restrictive.

## Quickstart 

Add it as a test dependency in Maven as:

```xml
<dependency>
  <groupId>com.github.fppt</groupId>
  <artifactId>jedis-mock</artifactId>
  <version>1.1.11</version>
  <scope>test</scope>
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
        server.getHost(), redisServer.getBindPort()));
RedissonClient client = Redisson.create(config);
```

From here test as needed.

## <a name="cluster">Cluster mode support</a>

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

## <a name="interceptor">Using `RedisCommandInterceptor`</a>

`RedisCommandInterceptor` is a functional interface which can be used to intercept calls to Jedis-Mock. 
You can use it as following:

```java
RedisServer server = RedisServer
    .newRedisServer()
    .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
        if ("get".equalsIgnoreCase(roName)) {
            //You can imitate any reply from Redis
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

## <a name="clockinjection">Clock injection</a>

[`java.time.Clock`](https://docs.oracle.com/javase/8/docs/api/java/time/Clock.html) injection is supported via `setClock` method on `RedisServer`. The injected clock is used for calculation and verification of the keys expiration time. Thus, it's possible to "freeze" or "skip" time for better testing of keys expiration. `setClock` is a thread-safe method which can be called as needed from any place of the code.

### "Freezing" time

```java
jedis.setex("key1", 1, "v1");
server.setClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()));
Thread.sleep(1500);
//The key must have expired, but the time is stopped
assertThat(jedis.exists("key1")).isTrue();
```
### Skipping time
```java
jedis.setex("key1", 20, "v1");
jedis.setex("key2", 40, "v2");

//Skipping 30 seconds: key1 expires, key2 not yet
server.setClock(Clock.offset(server.getClock(), Duration.ofSeconds(30)));
assertThat(jedis.exists("key1")).isFalse();
assertThat(jedis.exists("key2")).isTrue();

//Skipping another 30 seconds: both keys expire
server.setClock(Clock.offset(server.getClock(), Duration.ofSeconds(30)));
assertThat(jedis.exists("key1")).isFalse();
assertThat(jedis.exists("key2")).isFalse();
```

### Setting time in the past
```java
jedis.setex("key1", 1, "v1");
//Give extra two seconds for the key to expire
server.setClock(Clock.offset(server.getClock(), Duration.ofSeconds(-2)));
Thread.sleep(1500);
assertThat(jedis.exists("key1")).isTrue();
Thread.sleep(1600);
assertThat(jedis.exists("key1")).isFalse();
```
:warning: Setting time in the past does not "magically revive" the keys that have already been expired, although it provides extra time before the expiration of the keys still present in the database. Also, clock injection does not change the semantics of waiting operations: e.g. `BLPOP mylist 10` will be waiting for the period of 10 seconds regardless of the changes in the injected clock.

## Supported and Missing Operations

All currently supported and missing operations are listed [here](supported_operations.md).

If you get the following error:

```
Unsupported operation {}
```

please feel free to create an issue requesting the missing operation, 
or implement it yourself in interceptor and send us the code. It's fun!

