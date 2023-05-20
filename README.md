[![GitHub release](https://img.shields.io/github/release/fppt/jedis-mock.svg)](https://github.com/fppt/jedis-mock/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock)
[![Actions Status: build](https://github.com/fppt/jedis-mock/workflows/build/badge.svg)](https://github.com/fppt/jedis-mock/actions?query=workflow%3A"build") 

# Jedis-Mock

Jedis-Mock is a simple in-memory mock of Redis for Java testing, which can also work as test proxy. 
Despite its name, it works on network protocol level and can be used with any Redis client 
(be it `Jedis`, `Lettuce` or others).

When used as a mock, it allows you to test behaviour dependent on Redis without having to deploy an instance of Redis.

[List of currently supported operations](supported_operations.md).

## Why, if we already have TestContainers?
TestContainers is a great solution for integration tests with real services, including Redis.

However, sometimes we want to use mock or proxy for some of our tests for the following reasons:

* TestContainers require Docker. Jedis-Mock is just a Maven dependency which, when used as 'pure' mock, can be run on any machine, right now.
* TestContainers tests can be slow and extremely resource-consuming. Jedis-Mock tests are lightning fast, which
encourages developers to write more tests and run them more often.
* Redis running in TestContainers is a "black box". We cannot verify what was actually called. 
  We cannot interfere with the reply. All this we can do with testing mock/proxy.
* If you wish, you can use Jedis-Mock *together* with TestContainers, delegating command execution 
  to a real Redis instance, intercepting some of the calls when needed.



## Quickstart 

Add it as a dependency in Maven as:

```xml
<dependency>
  <groupId>com.github.fppt</groupId>
  <artifactId>jedis-mock</artifactId>
  <version>1.0.6</version>
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
```

From here test as needed.

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

WARNING: if you are going to mutate the shared state, synchronize on `state.lock()` first!
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

JedisMock supports Lua scripting (`EVAL`, `EVALSHA`, `SCRIPT LOAD/EXISTS/FLUSH` commands) via [luaj](https://github.com/luaj/luaj). In order to support these commands, [Jedis](https://github.com/redis/jedis) must be on the classpath as well. It is in the `provided` scope of Maven dependencies for JedisMock in order not to clash with the versions of Jedis you might be using. If you are experiencing "class not found" errors when running `EVAL`, [add Jedis dependency](https://github.com/redis/jedis#getting-started) to your test scope. 

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

## Supported and Missing Operations

All currently supported and missing operations are listed [here](supported_operations.md).

If you get the following error:

```
Unsupported operation {}
```

Please feel free to create an issue requesting the missing operation, 
or implement it yourself in interceptor and send us the code. It's fun!

