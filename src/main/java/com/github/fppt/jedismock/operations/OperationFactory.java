package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

public class OperationFactory {
    private static final Map<String, BiFunction<RedisBase, List<Slice>, RedisOperation>> TRANSACTIONAL_OPERATIONS = new HashMap<>();

    static {
        TRANSACTIONAL_OPERATIONS.put("set", RO_set::new);
        TRANSACTIONAL_OPERATIONS.put("setex", RO_setex::new);
        TRANSACTIONAL_OPERATIONS.put("psetex", RO_psetex::new);
        TRANSACTIONAL_OPERATIONS.put("setnx", RO_setnx::new);
        TRANSACTIONAL_OPERATIONS.put("setbit", RO_setbit::new);
        TRANSACTIONAL_OPERATIONS.put("append", RO_append::new);
        TRANSACTIONAL_OPERATIONS.put("get", RO_get::new);
        TRANSACTIONAL_OPERATIONS.put("getbit", RO_getbit::new);
        TRANSACTIONAL_OPERATIONS.put("ttl", RO_ttl::new);
        TRANSACTIONAL_OPERATIONS.put("pttl", RO_pttl::new);
        TRANSACTIONAL_OPERATIONS.put("expire", RO_expire::new);
        TRANSACTIONAL_OPERATIONS.put("pexpire", RO_pexpire::new);
        TRANSACTIONAL_OPERATIONS.put("incr", RO_incr::new);
        TRANSACTIONAL_OPERATIONS.put("incrby", RO_incrby::new);
        TRANSACTIONAL_OPERATIONS.put("incrbyfloat", RO_incrbyfloat::new);
        TRANSACTIONAL_OPERATIONS.put("decr", RO_decr::new);
        TRANSACTIONAL_OPERATIONS.put("decrby", RO_decrby::new);
        TRANSACTIONAL_OPERATIONS.put("pfcount", RO_pfcount::new);
        TRANSACTIONAL_OPERATIONS.put("pfadd", RO_pfadd::new);
        TRANSACTIONAL_OPERATIONS.put("pfmerge", RO_pfmerge::new);
        TRANSACTIONAL_OPERATIONS.put("mget", RO_mget::new);
        TRANSACTIONAL_OPERATIONS.put("mset", RO_mset::new);
        TRANSACTIONAL_OPERATIONS.put("getset", RO_getset::new);
        TRANSACTIONAL_OPERATIONS.put("strlen", RO_strlen::new);
        TRANSACTIONAL_OPERATIONS.put("del", RO_del::new);
        TRANSACTIONAL_OPERATIONS.put("exists", RO_exists::new);
        TRANSACTIONAL_OPERATIONS.put("expireat", RO_expireat::new);
        TRANSACTIONAL_OPERATIONS.put("pexpireat", RO_pexpireat::new);
        TRANSACTIONAL_OPERATIONS.put("lpush", RO_lpush::new);
        TRANSACTIONAL_OPERATIONS.put("rpush", RO_rpush::new);
        TRANSACTIONAL_OPERATIONS.put("lpushx", RO_lpushx::new);
        TRANSACTIONAL_OPERATIONS.put("lrange", RO_lrange::new);
        TRANSACTIONAL_OPERATIONS.put("llen", RO_llen::new);
        TRANSACTIONAL_OPERATIONS.put("lpop", RO_lpop::new);
        TRANSACTIONAL_OPERATIONS.put("rpop", RO_rpop::new);
        TRANSACTIONAL_OPERATIONS.put("lindex", RO_lindex::new);
        TRANSACTIONAL_OPERATIONS.put("rpoplpush", RO_rpoplpush::new);
        TRANSACTIONAL_OPERATIONS.put("brpoplpush", RO_brpoplpush::new);
        TRANSACTIONAL_OPERATIONS.put("publish", RO_publish::new);
        TRANSACTIONAL_OPERATIONS.put("flushdb", RO_flushdb::new);
        TRANSACTIONAL_OPERATIONS.put("dbsize", RO_dbsize::new);
        TRANSACTIONAL_OPERATIONS.put("lrem", RO_lrem::new);
        TRANSACTIONAL_OPERATIONS.put("ping", RO_ping::new);
        TRANSACTIONAL_OPERATIONS.put("keys", RO_keys::new);
        TRANSACTIONAL_OPERATIONS.put("sadd", RO_sadd::new);
        TRANSACTIONAL_OPERATIONS.put("scan", RO_scan::new);
        TRANSACTIONAL_OPERATIONS.put("sscan", RO_sscan::new);
        TRANSACTIONAL_OPERATIONS.put("spop", RO_spop::new);
        TRANSACTIONAL_OPERATIONS.put("srem", RO_srem::new);
        TRANSACTIONAL_OPERATIONS.put("scard", RO_scard::new);
        TRANSACTIONAL_OPERATIONS.put("sismember", RO_sismember::new);
        TRANSACTIONAL_OPERATIONS.put("hexists", RO_hexists::new);
        TRANSACTIONAL_OPERATIONS.put("hget", RO_hget::new);
        TRANSACTIONAL_OPERATIONS.put("hset", RO_hset::new);
        TRANSACTIONAL_OPERATIONS.put("hdel", RO_hdel::new);
        TRANSACTIONAL_OPERATIONS.put("hkeys", RO_hkeys::new);
        TRANSACTIONAL_OPERATIONS.put("hvals", RO_hvals::new);
        TRANSACTIONAL_OPERATIONS.put("hlen", RO_hlen::new);
        TRANSACTIONAL_OPERATIONS.put("hgetall", RO_hgetall::new);
        TRANSACTIONAL_OPERATIONS.put("hincrby", RO_hincrby::new);
        TRANSACTIONAL_OPERATIONS.put("hincrbyfloat", RO_hincrbyfloat::new);
        TRANSACTIONAL_OPERATIONS.put("hscan", RO_hscan::new);
        TRANSACTIONAL_OPERATIONS.put("sinter", RO_sinter::new);
        TRANSACTIONAL_OPERATIONS.put("hmget", RO_hmget::new);
        TRANSACTIONAL_OPERATIONS.put("hmset", RO_hmset::new);
        TRANSACTIONAL_OPERATIONS.put("smembers", RO_smembers::new);
        TRANSACTIONAL_OPERATIONS.put("hsetnx", RO_hsetnx::new);
        TRANSACTIONAL_OPERATIONS.put("time", RO_time::new);
        TRANSACTIONAL_OPERATIONS.put("blpop", RO_blpop::new);
        TRANSACTIONAL_OPERATIONS.put("brpop", RO_brpop::new);
        TRANSACTIONAL_OPERATIONS.put("zadd", RO_zadd::new);
        TRANSACTIONAL_OPERATIONS.put("zcard", RO_zcard::new);
        TRANSACTIONAL_OPERATIONS.put("zrange", RO_zrange::new);
        TRANSACTIONAL_OPERATIONS.put("zrangebylex", RO_zrangebylex::new);
        TRANSACTIONAL_OPERATIONS.put("zrevrangebylex", RO_zrevrangebylex::new);
        TRANSACTIONAL_OPERATIONS.put("zrem", RO_zrem::new);
        TRANSACTIONAL_OPERATIONS.put("zrangebyscore", RO_zrangebyscore::new);
        TRANSACTIONAL_OPERATIONS.put("zremrangebyscore", RO_zremrangebyscore::new);
        TRANSACTIONAL_OPERATIONS.put("rename", RO_rename::new);
        TRANSACTIONAL_OPERATIONS.put("zscore", RO_zscore::new);
        TRANSACTIONAL_OPERATIONS.put("type", RO_type::new);
    }


    public static RedisOperation buildTxOperation(RedisBase base, String name, List<Slice> params) {
        BiFunction<RedisBase, List<Slice>, RedisOperation> builder = OperationFactory.TRANSACTIONAL_OPERATIONS.get(name);
        if (builder == null) throw new UnsupportedOperationException(String.format("Unsupported operation '%s'", name));
        return builder.apply(base, params);
    }

    public static Optional<RedisOperation> buildMetaOperation(String name, OperationExecutorState state, List<Slice> params) {
        switch (name) {
            case "info":
                return Optional.of(new RO_info());
            case "multi":
                return Optional.of(new RO_multi(state));
            case "select":
                return Optional.of(new RO_select(state, params));
            case "subscribe":
                return Optional.of(new RO_subscribe(state, params));
            case "unsubscribe":
                return Optional.of(new RO_unsubscribe(state, params));
            case "quit":
                return Optional.of(new RO_quit(state));
            case "auth":
                return Optional.of(new RO_auth(state));
            case "exec":
                return Optional.of(new RO_exec(state));
            case "client":
                return Optional.of(new RO_client());
            case "hello":
                return Optional.of(new RO_hello());
            case "flushall":
                return Optional.of(new RO_flushall(state));
            default:
                return Optional.empty();
        }
    }


}
