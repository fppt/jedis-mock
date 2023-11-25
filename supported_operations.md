# Supported operations:

## Administration

:x: acl<br>
:x: bgrewriteaof<br>
:x: bgsave<br>
:heavy_check_mark: client<br>
:heavy_check_mark: cluster<br>
:x: config<br>
:x: debug<br>
:x: failover<br>
:x: lastsave<br>
:x: latency<br>
:x: module<br>
:x: monitor<br>
:x: pfdebug<br>
:x: pfselftest<br>
:x: psync<br>
:x: replconf<br>
:x: replicaof<br>
:x: role<br>
:x: save<br>
:x: shutdown<br>
:x: slaveof<br>
:x: slowlog<br>
:x: sync<br>

## Bitmaps

:x: bitcount<br>
:x: bitfield<br>
:x: bitfield_ro<br>
:x: bitop<br>
:x: bitpos<br>
:heavy_check_mark: getbit<br>
:heavy_check_mark: setbit<br>

## Connection

:x: asking<br>
:heavy_check_mark: auth<br>
:x: command<br>
:heavy_check_mark: echo<br>
:heavy_check_mark: hello<br>
:heavy_check_mark: ping<br>
:heavy_check_mark: quit<br>
:x: readonly<br>
:x: readwrite<br>
:x: reset<br>
:heavy_check_mark: select<br>
:x: wait<br>
:x: waitaof<br>

## Geo

:x: geoadd<br>
:x: geodist<br>
:x: geohash<br>
:x: geopos<br>
:x: georadius<br>
:x: georadius_ro<br>
:x: georadiusbymember<br>
:x: georadiusbymember_ro<br>
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

:x: copy<br>
:heavy_check_mark: dbsize<br>
:heavy_check_mark: del<br>
:x: dump<br>
:heavy_check_mark: exists<br>
:heavy_check_mark: expire<br>
:heavy_check_mark: expireat<br>
:x: expiretime<br>
:heavy_check_mark: flushall<br>
:heavy_check_mark: flushdb<br>
:heavy_check_mark: keys<br>
:x: migrate<br>
:x: move<br>
:x: object<br>
:heavy_check_mark: persist<br>
:heavy_check_mark: pexpire<br>
:heavy_check_mark: pexpireat<br>
:x: pexpiretime<br>
:heavy_check_mark: pttl<br>
:x: randomkey<br>
:heavy_check_mark: rename<br>
:x: renamenx<br>
:x: restore<br>
:x: restore-asking<br>
:heavy_check_mark: scan<br>
:x: swapdb<br>
:x: touch<br>
:heavy_check_mark: ttl<br>
:heavy_check_mark: type<br>
:heavy_check_mark: unlink<br>

## Lists

:x: blmove<br>
:x: blmpop<br>
:heavy_check_mark: blpop<br>
:heavy_check_mark: brpop<br>
:heavy_check_mark: brpoplpush<br>
:heavy_check_mark: lindex<br>
:heavy_check_mark: linsert<br>
:heavy_check_mark: llen<br>
:x: lmove<br>
:x: lmpop<br>
:heavy_check_mark: lpop<br>
:heavy_check_mark: lpos<br>
:heavy_check_mark: lpush<br>
:heavy_check_mark: lpushx<br>
:heavy_check_mark: lrange<br>
:heavy_check_mark: lrem<br>
:heavy_check_mark: lset<br>
:heavy_check_mark: ltrim<br>
:heavy_check_mark: rpop<br>
:heavy_check_mark: rpoplpush<br>
:heavy_check_mark: rpush<br>
:heavy_check_mark: rpushx<br>
:heavy_check_mark: sort<br>
:x: sort_ro<br>

## Pub/Sub

:heavy_check_mark: psubscribe<br>
:heavy_check_mark: publish<br>
:heavy_check_mark: pubsub<br>
:heavy_check_mark: punsubscribe<br>
:x: spublish<br>
:x: ssubscribe<br>
:heavy_check_mark: subscribe<br>
:x: sunsubscribe<br>
:heavy_check_mark: unsubscribe<br>

## Scripting

:heavy_check_mark: eval<br>
:x: eval_ro<br>
:heavy_check_mark: evalsha<br>
:x: evalsha_ro<br>
:x: fcall<br>
:x: fcall_ro<br>
:x: function<br>
:heavy_check_mark: script<br>

## Sets

:heavy_check_mark: sadd<br>
:heavy_check_mark: scard<br>
:heavy_check_mark: sdiff<br>
:heavy_check_mark: sdiffstore<br>
:heavy_check_mark: sinter<br>
:x: sintercard<br>
:heavy_check_mark: sinterstore<br>
:heavy_check_mark: sismember<br>
:heavy_check_mark: smembers<br>
:heavy_check_mark: smismember<br>
:heavy_check_mark: smove<br>
:heavy_check_mark: spop<br>
:heavy_check_mark: srandmember<br>
:heavy_check_mark: srem<br>
:heavy_check_mark: sscan<br>
:heavy_check_mark: sunion<br>
:heavy_check_mark: sunionstore<br>

## Sorted Sets

:x: bzmpop<br>
:x: bzpopmax<br>
:x: bzpopmin<br>
:heavy_check_mark: zadd<br>
:heavy_check_mark: zcard<br>
:heavy_check_mark: zcount<br>
:heavy_check_mark: zdiff<br>
:heavy_check_mark: zdiffstore<br>
:heavy_check_mark: zincrby<br>
:heavy_check_mark: zinter<br>
:heavy_check_mark: zintercard<br>
:heavy_check_mark: zinterstore<br>
:heavy_check_mark: zlexcount<br>
:heavy_check_mark: zmpop<br>
:heavy_check_mark: zmscore<br>
:heavy_check_mark: zpopmax<br>
:heavy_check_mark: zpopmin<br>
:x: zrandmember<br>
:heavy_check_mark: zrange<br>
:heavy_check_mark: zrangebylex<br>
:heavy_check_mark: zrangebyscore<br>
:heavy_check_mark: zrangestore<br>
:heavy_check_mark: zrank<br>
:heavy_check_mark: zrem<br>
:heavy_check_mark: zremrangebylex<br>
:heavy_check_mark: zremrangebyrank<br>
:heavy_check_mark: zremrangebyscore<br>
:heavy_check_mark: zrevrange<br>
:heavy_check_mark: zrevrangebylex<br>
:heavy_check_mark: zrevrangebyscore<br>
:heavy_check_mark: zrevrank<br>
:heavy_check_mark: zscan<br>
:heavy_check_mark: zscore<br>
:heavy_check_mark: zunion<br>
:heavy_check_mark: zunionstore<br>

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
:x: xsetid<br>
:x: xtrim<br>

## Strings

:heavy_check_mark: append<br>
:heavy_check_mark: decr<br>
:heavy_check_mark: decrby<br>
:heavy_check_mark: get<br>
:heavy_check_mark: getdel<br>
:x: getex<br>
:x: getrange<br>
:heavy_check_mark: getset<br>
:heavy_check_mark: incr<br>
:heavy_check_mark: incrby<br>
:heavy_check_mark: incrbyfloat<br>
:x: lcs<br>
:heavy_check_mark: mget<br>
:heavy_check_mark: mset<br>
:heavy_check_mark: msetnx<br>
:heavy_check_mark: psetex<br>
:heavy_check_mark: set<br>
:heavy_check_mark: setex<br>
:heavy_check_mark: setnx<br>
:heavy_check_mark: setrange<br>
:heavy_check_mark: strlen<br>
:x: substr<br>

## Transactions

:heavy_check_mark: discard<br>
:heavy_check_mark: exec<br>
:heavy_check_mark: multi<br>
:heavy_check_mark: unwatch<br>
:heavy_check_mark: watch<br>
