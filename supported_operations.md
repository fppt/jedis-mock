# Supported operations:

## Connection

:heavy_check_mark: auth<br>
:heavy_check_mark: client<br>
:x: echo<br>
:heavy_check_mark: hello<br>
:heavy_check_mark: ping<br>
:heavy_check_mark: quit<br>
:x: reset<br>
:heavy_check_mark: select<br>

## Cluster

:x: cluster<br>
:x: readonly<br>
:x: readwrite<br>

## Geo

:x: geoadd<br>
:x: geodist<br>
:x: geohash<br>
:x: geopos<br>
:x: georadius<br>
:x: georadiusbymember<br>
:x: geosearch<br>
:x: geosearchstore<br>

## Hashes

:heavy_check_mark: hdel<br>
:heavy_check_mark: hexists<br>
:heavy_check_mark: hget<br>
:heavy_check_mark: hgetall<br>
:heavy_check_mark: hincrby<br>
:heavy_check_mark: hincrbyfloat<br>
:heavy_check_mark: hkeys<br>
:heavy_check_mark: hlen<br>
:heavy_check_mark: hmget<br>
:heavy_check_mark: hmset<br>
:x: hrandfield<br>
:heavy_check_mark: hscan<br>
:heavy_check_mark: hset<br>
:heavy_check_mark: hsetnx<br>
:heavy_check_mark: hstrlen<br>
:heavy_check_mark: hvals<br>

## HyperLogLog

:heavy_check_mark: pfadd<br>
:heavy_check_mark: pfcount<br>
:heavy_check_mark: pfmerge<br>

## Keys

:x: asking<br>
:x: bitfield_ro<br>
:x: copy<br>
:heavy_check_mark: del<br>
:x: dump<br>
:heavy_check_mark: exists<br>
:heavy_check_mark: expire<br>
:heavy_check_mark: expireat<br>
:x: georadius_ro<br>
:x: georadiusbymember_ro<br>
:x: host:<br>
:heavy_check_mark: keys<br>
:x: migrate<br>
:x: move<br>
:x: object<br>
:heavy_check_mark: persist<br>
:heavy_check_mark: pexpire<br>
:heavy_check_mark: pexpireat<br>
:x: pfdebug<br>
:x: pfselftest<br>
:x: post<br>
:heavy_check_mark: pttl<br>
:x: randomkey<br>
:heavy_check_mark: rename<br>
:x: renamenx<br>
:x: replconf<br>
:x: restore<br>
:x: restore-asking<br>
:heavy_check_mark: scan<br>
:x: sort<br>
:x: substr<br>
:x: touch<br>
:heavy_check_mark: ttl<br>
:heavy_check_mark: type<br>
:x: unlink<br>
:x: wait<br>
:x: xsetid<br>

## Lists

:x: blmove<br>
:heavy_check_mark: blpop<br>
:heavy_check_mark: brpop<br>
:heavy_check_mark: brpoplpush<br>
:heavy_check_mark: lindex<br>
:x: linsert<br>
:heavy_check_mark: llen<br>
:x: lmove<br>
:heavy_check_mark: lpop<br>
:x: lpos<br>
:heavy_check_mark: lpush<br>
:heavy_check_mark: lpushx<br>
:heavy_check_mark: lrange<br>
:heavy_check_mark: lrem<br>
:x: lset<br>
:heavy_check_mark: ltrim<br>
:heavy_check_mark: rpop<br>
:heavy_check_mark: rpoplpush<br>
:heavy_check_mark: rpush<br>
:x: rpushx<br>

## Pub/Sub

:heavy_check_mark: psubscribe<br>
:heavy_check_mark: publish<br>
:heavy_check_mark: pubsub<br>
:heavy_check_mark: punsubscribe<br>
:heavy_check_mark: subscribe<br>
:heavy_check_mark: unsubscribe<br>

## Scripting

:x: eval<br>
:x: evalsha<br>
:x: script<br>

## Server

:x: acl<br>
:x: bgrewriteaof<br>
:x: bgsave<br>
:x: command<br>
:x: config<br>
:heavy_check_mark: dbsize<br>
:x: debug<br>
:x: failover<br>
:heavy_check_mark: flushall<br>
:heavy_check_mark: flushdb<br>
:heavy_check_mark: info<br>
:x: lastsave<br>
:x: latency<br>
:x: lolwut<br>
:x: memory<br>
:x: module<br>
:x: monitor<br>
:x: psync<br>
:x: replicaof<br>
:x: role<br>
:x: save<br>
:x: shutdown<br>
:x: slaveof<br>
:x: slowlog<br>
:x: swapdb<br>
:x: sync<br>
:heavy_check_mark: time<br>

## Sets

:heavy_check_mark: sadd<br>
:heavy_check_mark: scard<br>
:x: sdiff<br>
:x: sdiffstore<br>
:heavy_check_mark: sinter<br>
:x: sinterstore<br>
:heavy_check_mark: sismember<br>
:heavy_check_mark: smembers<br>
:x: smismember<br>
:x: smove<br>
:heavy_check_mark: spop<br>
:x: srandmember<br>
:heavy_check_mark: srem<br>
:heavy_check_mark: sscan<br>
:x: sunion<br>
:x: sunionstore<br>

## Sorted Sets

:x: bzpopmax<br>
:x: bzpopmin<br>
:heavy_check_mark: zadd<br>
:heavy_check_mark: zcard<br>
:heavy_check_mark: zcount<br>
:x: zdiff<br>
:x: zdiffstore<br>
:x: zincrby<br>
:x: zinter<br>
:x: zinterstore<br>
:x: zlexcount<br>
:x: zmscore<br>
:x: zpopmax<br>
:x: zpopmin<br>
:x: zrandmember<br>
:heavy_check_mark: zrange<br>
:heavy_check_mark: zrangebylex<br>
:heavy_check_mark: zrangebyscore<br>
:x: zrangestore<br>
:x: zrank<br>
:heavy_check_mark: zrem<br>
:x: zremrangebylex<br>
:x: zremrangebyrank<br>
:heavy_check_mark: zremrangebyscore<br>
:heavy_check_mark: zrevrange<br>
:heavy_check_mark: zrevrangebylex<br>
:heavy_check_mark: zrevrangebyscore<br>
:x: zrevrank<br>
:x: zscan<br>
:heavy_check_mark: zscore<br>
:x: zunion<br>
:x: zunionstore<br>

## Streams

:x: xack<br>
:x: xadd<br>
:x: xautoclaim<br>
:x: xclaim<br>
:x: xdel<br>
:x: xgroup<br>
:x: xinfo<br>
:x: xlen<br>
:x: xpending<br>
:x: xrange<br>
:x: xread<br>
:x: xreadgroup<br>
:x: xrevrange<br>
:x: xtrim<br>

## Strings

:heavy_check_mark: append<br>
:x: bitcount<br>
:x: bitfield<br>
:x: bitop<br>
:x: bitpos<br>
:heavy_check_mark: decr<br>
:heavy_check_mark: decrby<br>
:heavy_check_mark: get<br>
:heavy_check_mark: getbit<br>
:x: getdel<br>
:x: getex<br>
:x: getrange<br>
:heavy_check_mark: getset<br>
:heavy_check_mark: incr<br>
:heavy_check_mark: incrby<br>
:heavy_check_mark: incrbyfloat<br>
:heavy_check_mark: mget<br>
:heavy_check_mark: mset<br>
:x: msetnx<br>
:heavy_check_mark: psetex<br>
:heavy_check_mark: set<br>
:heavy_check_mark: setbit<br>
:heavy_check_mark: setex<br>
:heavy_check_mark: setnx<br>
:x: setrange<br>
:x: stralgo<br>
:heavy_check_mark: strlen<br>

## Transactions

:heavy_check_mark: discard<br>
:heavy_check_mark: exec<br>
:heavy_check_mark: multi<br>
:heavy_check_mark: unwatch<br>
:heavy_check_mark: watch<br>
