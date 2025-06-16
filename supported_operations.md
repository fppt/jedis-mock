# Supported operations:

## Administration

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :x: [acl](https://valkey.io/commands/acl/) | :heavy_check_mark: [cluster](https://valkey.io/commands/cluster/) | :x: [lastsave](https://valkey.io/commands/lastsave/) | :x: [pfdebug](https://valkey.io/commands/pfdebug/) | :x: [replicaof](https://valkey.io/commands/replicaof/) | :x: [slaveof](https://valkey.io/commands/slaveof/) |
| :x: [bgrewriteaof](https://valkey.io/commands/bgrewriteaof/) | :x: [config](https://valkey.io/commands/config/) | :x: [latency](https://valkey.io/commands/latency/) | :x: [pfselftest](https://valkey.io/commands/pfselftest/) | :x: [role](https://valkey.io/commands/role/) | :x: [slowlog](https://valkey.io/commands/slowlog/) |
| :x: [bgsave](https://valkey.io/commands/bgsave/) | :x: [debug](https://valkey.io/commands/debug/) | :x: [module](https://valkey.io/commands/module/) | :x: [psync](https://valkey.io/commands/psync/) | :x: [save](https://valkey.io/commands/save/) | :x: [sync](https://valkey.io/commands/sync/) |
| :heavy_check_mark: [client](https://valkey.io/commands/client/) | :x: [failover](https://valkey.io/commands/failover/) | :x: [monitor](https://valkey.io/commands/monitor/) | :x: [replconf](https://valkey.io/commands/replconf/) | :x: [shutdown](https://valkey.io/commands/shutdown/) |  |

## Bitmaps

|     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- |
| :x: [bitcount](https://valkey.io/commands/bitcount/) | :x: [bitfield](https://valkey.io/commands/bitfield/) | :x: [bitfield_ro](https://valkey.io/commands/bitfield_ro/) | :x: [bitop](https://valkey.io/commands/bitop/) | :x: [bitpos](https://valkey.io/commands/bitpos/) | :heavy_check_mark: [getbit](https://valkey.io/commands/getbit/) | :heavy_check_mark: [setbit](https://valkey.io/commands/setbit/) |

## Connection

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :x: [asking](https://valkey.io/commands/asking/) | :heavy_check_mark: [echo](https://valkey.io/commands/echo/) | :heavy_check_mark: [ping](https://valkey.io/commands/ping/) | :x: [readonly](https://valkey.io/commands/readonly/) | :x: [reset](https://valkey.io/commands/reset/) | :x: [wait](https://valkey.io/commands/wait/) |
| :heavy_check_mark: [auth](https://valkey.io/commands/auth/) | :heavy_check_mark: [hello](https://valkey.io/commands/hello/) | :heavy_check_mark: [quit](https://valkey.io/commands/quit/) | :x: [readwrite](https://valkey.io/commands/readwrite/) | :heavy_check_mark: [select](https://valkey.io/commands/select/) | :x: [waitaof](https://valkey.io/commands/waitaof/) |
| :x: [command](https://valkey.io/commands/command/) |  |  |  |  |  |

## Geo

|     |     |     |     |     |
| --- | --- | --- | --- | --- |
| :x: [geoadd](https://valkey.io/commands/geoadd/) | :x: [geohash](https://valkey.io/commands/geohash/) | :x: [georadius](https://valkey.io/commands/georadius/) | :x: [georadiusbymember](https://valkey.io/commands/georadiusbymember/) | :x: [geosearch](https://valkey.io/commands/geosearch/) |
| :x: [geodist](https://valkey.io/commands/geodist/) | :x: [geopos](https://valkey.io/commands/geopos/) | :x: [georadius_ro](https://valkey.io/commands/georadius_ro/) | :x: [georadiusbymember_ro](https://valkey.io/commands/georadiusbymember_ro/) | :x: [geosearchstore](https://valkey.io/commands/geosearchstore/) |

## Hashes

|     |     |     |     |     |
| --- | --- | --- | --- | --- |
| :heavy_check_mark: [hdel](https://valkey.io/commands/hdel/) | :heavy_check_mark: [hget](https://valkey.io/commands/hget/) | :heavy_check_mark: [hlen](https://valkey.io/commands/hlen/) | :heavy_check_mark: [hpexpireat](https://valkey.io/commands/hpexpireat/) | :heavy_check_mark: [hset](https://valkey.io/commands/hset/) |
| :heavy_check_mark: [hexists](https://valkey.io/commands/hexists/) | :heavy_check_mark: [hgetall](https://valkey.io/commands/hgetall/) | :heavy_check_mark: [hmget](https://valkey.io/commands/hmget/) | :heavy_check_mark: [hpexpiretime](https://valkey.io/commands/hpexpiretime/) | :heavy_check_mark: [hsetnx](https://valkey.io/commands/hsetnx/) |
| :heavy_check_mark: [hexpire](https://valkey.io/commands/hexpire/) | :heavy_check_mark: [hincrby](https://valkey.io/commands/hincrby/) | :heavy_check_mark: [hmset](https://valkey.io/commands/hmset/) | :heavy_check_mark: [hpttl](https://valkey.io/commands/hpttl/) | :heavy_check_mark: [hstrlen](https://valkey.io/commands/hstrlen/) |
| :heavy_check_mark: [hexpireat](https://valkey.io/commands/hexpireat/) | :heavy_check_mark: [hincrbyfloat](https://valkey.io/commands/hincrbyfloat/) | :heavy_check_mark: [hpersist](https://valkey.io/commands/hpersist/) | :x: [hrandfield](https://valkey.io/commands/hrandfield/) | :heavy_check_mark: [httl](https://valkey.io/commands/httl/) |
| :heavy_check_mark: [hexpiretime](https://valkey.io/commands/hexpiretime/) | :heavy_check_mark: [hkeys](https://valkey.io/commands/hkeys/) | :heavy_check_mark: [hpexpire](https://valkey.io/commands/hpexpire/) | :heavy_check_mark: [hscan](https://valkey.io/commands/hscan/) | :heavy_check_mark: [hvals](https://valkey.io/commands/hvals/) |

## HyperLogLog

|     |     |     |
| --- | --- | --- |
| :heavy_check_mark: [pfadd](https://valkey.io/commands/pfadd/) | :heavy_check_mark: [pfcount](https://valkey.io/commands/pfcount/) | :heavy_check_mark: [pfmerge](https://valkey.io/commands/pfmerge/) |

## Keys

|     |     |     |     |     |
| --- | --- | --- | --- | --- |
| :heavy_check_mark: [copy](https://valkey.io/commands/copy/) | :heavy_check_mark: [expireat](https://valkey.io/commands/expireat/) | :heavy_check_mark: [move](https://valkey.io/commands/move/) | :heavy_check_mark: [pttl](https://valkey.io/commands/pttl/) | :heavy_check_mark: [scan](https://valkey.io/commands/scan/) |
| :heavy_check_mark: [dbsize](https://valkey.io/commands/dbsize/) | :heavy_check_mark: [expiretime](https://valkey.io/commands/expiretime/) | :x: [object](https://valkey.io/commands/object/) | :x: [randomkey](https://valkey.io/commands/randomkey/) | :x: [swapdb](https://valkey.io/commands/swapdb/) |
| :heavy_check_mark: [del](https://valkey.io/commands/del/) | :heavy_check_mark: [flushall](https://valkey.io/commands/flushall/) | :heavy_check_mark: [persist](https://valkey.io/commands/persist/) | :heavy_check_mark: [rename](https://valkey.io/commands/rename/) | :x: [touch](https://valkey.io/commands/touch/) |
| :x: [dump](https://valkey.io/commands/dump/) | :heavy_check_mark: [flushdb](https://valkey.io/commands/flushdb/) | :heavy_check_mark: [pexpire](https://valkey.io/commands/pexpire/) | :x: [renamenx](https://valkey.io/commands/renamenx/) | :heavy_check_mark: [ttl](https://valkey.io/commands/ttl/) |
| :heavy_check_mark: [exists](https://valkey.io/commands/exists/) | :heavy_check_mark: [keys](https://valkey.io/commands/keys/) | :heavy_check_mark: [pexpireat](https://valkey.io/commands/pexpireat/) | :x: [restore](https://valkey.io/commands/restore/) | :heavy_check_mark: [type](https://valkey.io/commands/type/) |
| :heavy_check_mark: [expire](https://valkey.io/commands/expire/) | :x: [migrate](https://valkey.io/commands/migrate/) | :heavy_check_mark: [pexpiretime](https://valkey.io/commands/pexpiretime/) | :x: [restore-asking](https://valkey.io/commands/restore-asking/) | :heavy_check_mark: [unlink](https://valkey.io/commands/unlink/) |

## Lists

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :x: [blmove](https://valkey.io/commands/blmove/) | :heavy_check_mark: [brpoplpush](https://valkey.io/commands/brpoplpush/) | :x: [lmove](https://valkey.io/commands/lmove/) | :heavy_check_mark: [lpush](https://valkey.io/commands/lpush/) | :heavy_check_mark: [lset](https://valkey.io/commands/lset/) | :heavy_check_mark: [rpush](https://valkey.io/commands/rpush/) |
| :x: [blmpop](https://valkey.io/commands/blmpop/) | :heavy_check_mark: [lindex](https://valkey.io/commands/lindex/) | :x: [lmpop](https://valkey.io/commands/lmpop/) | :heavy_check_mark: [lpushx](https://valkey.io/commands/lpushx/) | :heavy_check_mark: [ltrim](https://valkey.io/commands/ltrim/) | :heavy_check_mark: [rpushx](https://valkey.io/commands/rpushx/) |
| :heavy_check_mark: [blpop](https://valkey.io/commands/blpop/) | :heavy_check_mark: [linsert](https://valkey.io/commands/linsert/) | :heavy_check_mark: [lpop](https://valkey.io/commands/lpop/) | :heavy_check_mark: [lrange](https://valkey.io/commands/lrange/) | :heavy_check_mark: [rpop](https://valkey.io/commands/rpop/) | :heavy_check_mark: [sort](https://valkey.io/commands/sort/) |
| :heavy_check_mark: [brpop](https://valkey.io/commands/brpop/) | :heavy_check_mark: [llen](https://valkey.io/commands/llen/) | :heavy_check_mark: [lpos](https://valkey.io/commands/lpos/) | :heavy_check_mark: [lrem](https://valkey.io/commands/lrem/) | :heavy_check_mark: [rpoplpush](https://valkey.io/commands/rpoplpush/) | :x: [sort_ro](https://valkey.io/commands/sort_ro/) |

## Pub/Sub

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :heavy_check_mark: [psubscribe](https://valkey.io/commands/psubscribe/) | :heavy_check_mark: [pubsub](https://valkey.io/commands/pubsub/) | :x: [spublish](https://valkey.io/commands/spublish/) | :heavy_check_mark: [subscribe](https://valkey.io/commands/subscribe/) | :x: [sunsubscribe](https://valkey.io/commands/sunsubscribe/) | :heavy_check_mark: [unsubscribe](https://valkey.io/commands/unsubscribe/) |
| :heavy_check_mark: [publish](https://valkey.io/commands/publish/) | :heavy_check_mark: [punsubscribe](https://valkey.io/commands/punsubscribe/) | :x: [ssubscribe](https://valkey.io/commands/ssubscribe/) |  |  |  |

## Scripting

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :heavy_check_mark: [eval](https://valkey.io/commands/eval/) | :heavy_check_mark: [evalsha](https://valkey.io/commands/evalsha/) | :x: [fcall](https://valkey.io/commands/fcall/) | :x: [fcall_ro](https://valkey.io/commands/fcall_ro/) | :x: [function](https://valkey.io/commands/function/) | :heavy_check_mark: [script](https://valkey.io/commands/script/) |
| :x: [eval_ro](https://valkey.io/commands/eval_ro/) | :x: [evalsha_ro](https://valkey.io/commands/evalsha_ro/) |  |  |  |  |

## Sets

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :heavy_check_mark: [sadd](https://valkey.io/commands/sadd/) | :heavy_check_mark: [sdiffstore](https://valkey.io/commands/sdiffstore/) | :heavy_check_mark: [sinterstore](https://valkey.io/commands/sinterstore/) | :heavy_check_mark: [smismember](https://valkey.io/commands/smismember/) | :heavy_check_mark: [srandmember](https://valkey.io/commands/srandmember/) | :heavy_check_mark: [sunion](https://valkey.io/commands/sunion/) |
| :heavy_check_mark: [scard](https://valkey.io/commands/scard/) | :heavy_check_mark: [sinter](https://valkey.io/commands/sinter/) | :heavy_check_mark: [sismember](https://valkey.io/commands/sismember/) | :heavy_check_mark: [smove](https://valkey.io/commands/smove/) | :heavy_check_mark: [srem](https://valkey.io/commands/srem/) | :heavy_check_mark: [sunionstore](https://valkey.io/commands/sunionstore/) |
| :heavy_check_mark: [sdiff](https://valkey.io/commands/sdiff/) | :x: [sintercard](https://valkey.io/commands/sintercard/) | :heavy_check_mark: [smembers](https://valkey.io/commands/smembers/) | :heavy_check_mark: [spop](https://valkey.io/commands/spop/) | :heavy_check_mark: [sscan](https://valkey.io/commands/sscan/) |  |

## Sorted Sets

|     |     |     |     |     |
| --- | --- | --- | --- | --- |
| :heavy_check_mark: [bzmpop](https://valkey.io/commands/bzmpop/) | :heavy_check_mark: [zdiffstore](https://valkey.io/commands/zdiffstore/) | :heavy_check_mark: [zmscore](https://valkey.io/commands/zmscore/) | :heavy_check_mark: [zrangestore](https://valkey.io/commands/zrangestore/) | :heavy_check_mark: [zrevrangebylex](https://valkey.io/commands/zrevrangebylex/) |
| :heavy_check_mark: [bzpopmax](https://valkey.io/commands/bzpopmax/) | :heavy_check_mark: [zincrby](https://valkey.io/commands/zincrby/) | :heavy_check_mark: [zpopmax](https://valkey.io/commands/zpopmax/) | :heavy_check_mark: [zrank](https://valkey.io/commands/zrank/) | :heavy_check_mark: [zrevrangebyscore](https://valkey.io/commands/zrevrangebyscore/) |
| :heavy_check_mark: [bzpopmin](https://valkey.io/commands/bzpopmin/) | :heavy_check_mark: [zinter](https://valkey.io/commands/zinter/) | :heavy_check_mark: [zpopmin](https://valkey.io/commands/zpopmin/) | :heavy_check_mark: [zrem](https://valkey.io/commands/zrem/) | :heavy_check_mark: [zrevrank](https://valkey.io/commands/zrevrank/) |
| :heavy_check_mark: [zadd](https://valkey.io/commands/zadd/) | :heavy_check_mark: [zintercard](https://valkey.io/commands/zintercard/) | :heavy_check_mark: [zrandmember](https://valkey.io/commands/zrandmember/) | :heavy_check_mark: [zremrangebylex](https://valkey.io/commands/zremrangebylex/) | :heavy_check_mark: [zscan](https://valkey.io/commands/zscan/) |
| :heavy_check_mark: [zcard](https://valkey.io/commands/zcard/) | :heavy_check_mark: [zinterstore](https://valkey.io/commands/zinterstore/) | :heavy_check_mark: [zrange](https://valkey.io/commands/zrange/) | :heavy_check_mark: [zremrangebyrank](https://valkey.io/commands/zremrangebyrank/) | :heavy_check_mark: [zscore](https://valkey.io/commands/zscore/) |
| :heavy_check_mark: [zcount](https://valkey.io/commands/zcount/) | :heavy_check_mark: [zlexcount](https://valkey.io/commands/zlexcount/) | :heavy_check_mark: [zrangebylex](https://valkey.io/commands/zrangebylex/) | :heavy_check_mark: [zremrangebyscore](https://valkey.io/commands/zremrangebyscore/) | :heavy_check_mark: [zunion](https://valkey.io/commands/zunion/) |
| :heavy_check_mark: [zdiff](https://valkey.io/commands/zdiff/) | :heavy_check_mark: [zmpop](https://valkey.io/commands/zmpop/) | :heavy_check_mark: [zrangebyscore](https://valkey.io/commands/zrangebyscore/) | :heavy_check_mark: [zrevrange](https://valkey.io/commands/zrevrange/) | :heavy_check_mark: [zunionstore](https://valkey.io/commands/zunionstore/) |

## Streams

|     |     |     |     |     |
| --- | --- | --- | --- | --- |
| :x: [xack](https://valkey.io/commands/xack/) | :x: [xclaim](https://valkey.io/commands/xclaim/) | :x: [xinfo](https://valkey.io/commands/xinfo/) | :heavy_check_mark: [xrange](https://valkey.io/commands/xrange/) | :heavy_check_mark: [xrevrange](https://valkey.io/commands/xrevrange/) |
| :heavy_check_mark: [xadd](https://valkey.io/commands/xadd/) | :heavy_check_mark: [xdel](https://valkey.io/commands/xdel/) | :heavy_check_mark: [xlen](https://valkey.io/commands/xlen/) | :heavy_check_mark: [xread](https://valkey.io/commands/xread/) | :x: [xsetid](https://valkey.io/commands/xsetid/) |
| :x: [xautoclaim](https://valkey.io/commands/xautoclaim/) | :x: [xgroup](https://valkey.io/commands/xgroup/) | :x: [xpending](https://valkey.io/commands/xpending/) | :x: [xreadgroup](https://valkey.io/commands/xreadgroup/) | :heavy_check_mark: [xtrim](https://valkey.io/commands/xtrim/) |

## Strings

|     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
| :heavy_check_mark: [append](https://valkey.io/commands/append/) | :heavy_check_mark: [getdel](https://valkey.io/commands/getdel/) | :heavy_check_mark: [incr](https://valkey.io/commands/incr/) | :heavy_check_mark: [mget](https://valkey.io/commands/mget/) | :heavy_check_mark: [set](https://valkey.io/commands/set/) | :heavy_check_mark: [setrange](https://valkey.io/commands/setrange/) |
| :heavy_check_mark: [decr](https://valkey.io/commands/decr/) | :x: [getex](https://valkey.io/commands/getex/) | :heavy_check_mark: [incrby](https://valkey.io/commands/incrby/) | :heavy_check_mark: [mset](https://valkey.io/commands/mset/) | :heavy_check_mark: [setex](https://valkey.io/commands/setex/) | :heavy_check_mark: [strlen](https://valkey.io/commands/strlen/) |
| :heavy_check_mark: [decrby](https://valkey.io/commands/decrby/) | :x: [getrange](https://valkey.io/commands/getrange/) | :heavy_check_mark: [incrbyfloat](https://valkey.io/commands/incrbyfloat/) | :heavy_check_mark: [msetnx](https://valkey.io/commands/msetnx/) | :heavy_check_mark: [setnx](https://valkey.io/commands/setnx/) | :x: [substr](https://valkey.io/commands/substr/) |
| :heavy_check_mark: [get](https://valkey.io/commands/get/) | :heavy_check_mark: [getset](https://valkey.io/commands/getset/) | :x: [lcs](https://valkey.io/commands/lcs/) | :heavy_check_mark: [psetex](https://valkey.io/commands/psetex/) |  |  |

## Transactions

|     |     |     |     |     |
| --- | --- | --- | --- | --- |
| :heavy_check_mark: [discard](https://valkey.io/commands/discard/) | :heavy_check_mark: [exec](https://valkey.io/commands/exec/) | :heavy_check_mark: [multi](https://valkey.io/commands/multi/) | :heavy_check_mark: [unwatch](https://valkey.io/commands/unwatch/) | :heavy_check_mark: [watch](https://valkey.io/commands/watch/) |
