[![GitHub release](https://img.shields.io/github/release/fppt/jedis-mock.svg)](https://github.com/fppt/jedis-mock/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.fppt/jedis-mock)
[![Actions Status: build](https://github.com/fppt/jedis-mock/workflows/build/badge.svg)](https://github.com/fppt/jedis-mock/actions?query=workflow%3A"build") 

# Jedis-Mock

Redis Mock is a simple in-memory mock of redis for java testing. 
It allows you to test any behaviour dependent on redis without having to deploy an instance of redis

## Quickstart 

Add it as a dependency in Maven as:

```xml
<dependency>
  <groupId>com.github.fppt</groupId>
  <artifactId>jedis-mock</artifactId>
  <version>0.1.22</version>
</dependency>
```

Create a redis server and bind it to jedis:

```
private static RedisServer server = null;

@Before
public void before() throws IOException {
  server = RedisServer.newRedisServer();  // bind to a random port
  server.start();
}

@Test
public void test() {
  ...
  Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
  ...
}

@After
public void after() {
  server.stop();
  server = null;
}
```

From here test as needed

## Fault tolerance testing

We can make a RedisServer close connection after every several commands. This will cause a connection exception for clients.

```
RedisServer server = RedisServer.newRedisServer();
ServiceOptions options = new ServiceOptions();
options.setCloseSocketAfterSeveralCommands(3);
server.setOptions(options);
server.start();
```

## Supported Operations

All currently supported operations are listed [here](supported_operations.md)

## Missing Operations

If you get the following error:

```
Unsupported operation {}
```

Please feel free to create an issue requesting the missing operation.

