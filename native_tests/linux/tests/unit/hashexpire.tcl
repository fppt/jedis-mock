proc info_field {info field} {
    foreach line [split $info "\n"] {
        if {[string match "$field:*" $line]} {
            return [string trim [lindex [split $line ":"] 1]]
        }
    }
    return [s field_name]
}

proc get_keys_with_volatile_items {r} {
    set line [$r info keyspace]
    set match [regexp -inline {keys_with_volatile_items=([\d]+)} $line]

    if {[llength $match] == 2} {
        return [lindex $match 1]
    } else {
        return 0
    }
}

proc get_keys {r} {
    set line [$r info keyspace]
    set match [regexp -inline {keys=([\d]+)} $line]

    if {[llength $match] == 2} {
        return [lindex $match 1]
    } else {
        return 0
    }
}

proc get_longer_then_long_expire_value {command} {
    expr {
        ($command eq "HEXPIRE" || $command eq "EX") ? 1200000000 :
        ($command eq "HPEXPIRE" || $command eq "PX") ? 1200000000 :
        ($command eq "HEXPIREAT" || $command eq "EXAT") ? [clock seconds] + 1200000000 :
        [clock milliseconds] + 1200000000
    }
}

proc get_past_zero_expire_value {command} {
    expr {
        ($command eq "HEXPIRE" || $command eq "EX") ? 0 :
        ($command eq "HPEXPIRE" || $command eq "PX") ? 0 :
        ($command eq "HEXPIREAT" || $command eq "EXAT") ? [clock seconds] - 200000 :
        [clock milliseconds] - 200000
    }
}

proc assert_keyevent_patterns {rd key args} {
    foreach event_type $args {
        set event [$rd read]
        assert_match "pmessage __keyevent@* __keyevent@*:$event_type $key" $event
    }
}

proc setup_single_keyspace_notification {r} {
    $r config set notify-keyspace-events KEA
    set rd [valkey_deferring_client]
    assert_equal {1} [psubscribe $rd __keyevent@*]
    return $rd
}

start_server {tags {"hashexpire"}} {
    # HSETEX ####
    test {HSETEX KEEPTTL - preserves existing TTL of field} {
        r FLUSHALL

        # Set a field with a known TTL
        r HSETEX myhash PX 1000 FIELDS 1 field1 val1
        set original_pttl [r HPTTL myhash FIELDS 1 field1]
        set original_expiretime [r HEXPIRETIME myhash FIELDS 1 field1]
        #assert_equal 1 [get_keys_with_volatile_items r]

        # Validate TTL is active and expiretime is in the future
        assert {$original_pttl > 0}
        assert {$original_expiretime > [clock seconds]}

        # Overwrite the field with KEEPTTL
        r HSETEX myhash KEEPTTL FIELDS 1 field1 newval

        # Ensure TTL is preserved
        set updated_pttl [r HPTTL myhash FIELDS 1 field1]
        set updated_expiretime [r HEXPIRETIME myhash FIELDS 1 field1]
        assert {$updated_pttl > 0}
        assert {$updated_pttl <= $original_pttl}
        assert_equal $original_expiretime $updated_expiretime

        # Ensure value was updated
        assert_equal newval [r HGET myhash field1]
    }

    test {HSETEX EX - FIELDS 0 returns error} {
        r FLUSHALL
        catch {r HSETEX myhash EX 10 FIELDS 0} e
        set e
    } {ERR *}

    test {HSETEX EX - test negative ttl} {
        set ttl -10
        catch {r HSETEX myhash EX $ttl FIELDS 1 field1 val1} e
        set e
    # altered from the original valkey assertion to allow more leniency on the error message, due to redis/valkey discrepancies.
    } {ERR invalid expire time*}

    test {HSETEX EX - test non-numeric ttl} {
        set ttl abc
        catch {r HSETEX myhash EX $ttl FIELDS 1 field1 val1} e
        set e
    } {ERR value is not an integer or out of range}

    test {HSETEX EX - overwrite field resets TTL} {
        r FLUSHALL
        r HSETEX myhash EX 100 FIELDS 1 field1 val1
        r HSETEX myhash EX 200 FIELDS 1 field1 newval
        assert_equal 200 [r HTTL myhash FIELDS 1 field1]
        assert_equal newval [r HGET myhash field1]
    }

    test {HSETEX EX - test mix of expiring and persistent fields} {
        r FLUSHALL
        r HSET myhash field2 "persistent"
        r HSETEX myhash EX 1 FIELDS 1 field1 "temp"
        #assert_equal 1 [get_keys_with_volatile_items r]
        after 1100
        assert_equal 0 [r HEXISTS myhash field1]
        assert_equal 1 [r HEXISTS myhash field2]
    }

    test {HSETEX EX - test missing TTL} {
        catch {r HSETEX myhash EX FIELDS 1 field1 val1} e
        set e
    } {ERR *}

    test {HSETEX EX - mismatched field/value count} {
        catch {r HSETEX myhash EX 10 FIELDS 2 field1 val1} e
        set e
    } {ERR *}

    foreach command {EX PX EXAT PXAT} {
        test "HSETEX $command 0/past time works correctly with 2 fields" {
            r FLUSHALL
            r config resetstat
            # Create hash with field
            r HSET myhash f1 v1
            assert_equal 1 [r HLEN myhash]
            #assert_equal 0 [get_keys_with_volatile_items r]
            #assert_equal 1 [get_keys r]
            set rd [setup_single_keyspace_notification r]

            # Set field to expire immediately
            assert_equal {1} [r HSETEX myhash $command [get_past_zero_expire_value $command] FIELDS 2 f1 v1 f2 v2]

            # Verify field and keys are deleted
            #assert_keyevent_patterns $rd myhash hset hexpire hexpired del
            assert_keyevent_patterns $rd myhash hset
            assert_equal -2 [r HTTL myhash FIELDS 1 f1]
            # jedis-mock deletes expired hashes only once they've been expired for at least a minute
            #assert_equal 0 [r HLEN myhash]
            #assert_equal 0 [r EXISTS myhash]
            #assert_equal 0 [get_keys r]
            #assert_equal 0 [get_keys_with_volatile_items r]
            #assert_equal 2 [info_field [r info stats] expired_fields]
            $rd close
        }
    }

    ###### PX #######

    test {HSETEX PX - test negative ttl} {
        set ttl -50
        catch {r HSETEX myhash PX $ttl FIELDS 1 field1 val1} e
        set e
    # altered from the original valkey assertion to allow more leniency on the error message, due to redis/valkey discrepancies.
    } {ERR invalid expire time*}

    test {HSETEX PX - test non-numeric ttl} {
        set ttl xyz
        catch {r HSETEX myhash PX $ttl FIELDS 1 field1 val1} e
        set e
    } {ERR value is not an integer or out of range}

    test {HSETEX PX - overwrite field resets TTL} {
        r FLUSHALL
        r HSETEX myhash PX 10000 FIELDS 1 field1 val1
        r HSETEX myhash PX 20000 FIELDS 1 field1 newval
        set ttl [r HPTTL myhash FIELDS 1 field1]
        assert {$ttl >= 19000 && $ttl <= 20000}
        assert_equal newval [r HGET myhash field1]
        #assert_equal 1 [get_keys_with_volatile_items r]
    }

    test {HSETEX PX - test zero ttl expires immediately} {
        r FLUSHALL
        r HSETEX myhash PX 0 FIELDS 1 field1 val1
        after 10
        assert_equal 0 [r HEXISTS myhash field1]
        # The hash should also not exist
        assert_equal 0 [r EXISTS myhash]
        assert_equal 0 [r HLEN myhash]
    }

    test {HSETEX PX - test mix of expiring and persistent fields} {
        r FLUSHALL
        r HSET myhash field2 "persistent"
        r HSETEX myhash PX 10 FIELDS 1 field1 "temp"
        after 20
        assert_equal 0 [r HEXISTS myhash field1]
        assert_equal 1 [r HEXISTS myhash field2]
    }

    test {HSETEX PX - test missing TTL} {
        catch {r HSETEX myhash PX FIELDS 1 field1 val1} e
        set e
    } {ERR *}

    test {HSETEX PX - mismatched field/value count} {
        # altered from the original valkey assertion to allow more leniency on the error message, due to redis/valkey discrepancies.
         assert_error {ERR*} {r HSETEX myhash PX 100 FIELDS 1 field1 val1 extra}
    }

    ## NX/XX key-level tests

    #test {HSETEX NX - non-existing key creates the key} {
    #    r FLUSHALL
    #    set res [r HSETEX myhash NX FIELDS 2 f1 v1 f2 v2]
    #    assert_equal 1 $res
    #    assert_equal v1 [r HGET myhash f1]
    #    assert_equal v2 [r HGET myhash f2]
    #}

    #test {HSETEX NX - existing key blocked} {
    #    r FLUSHALL
    #    r HSET myhash f1 v1
    #    set res [r HSETEX myhash NX FIELDS 2 f1 new1 f2 new2]
    #    assert_equal 0 $res
    #    assert_equal v1 [r HGET myhash f1]
    #    assert_equal 0 [r HEXISTS myhash f2]
    #}

    #test {HSETEX XX - existing key updates fields} {
    #    r FLUSHALL
    #    r HSET myhash f1 v1 f2 v2
    #    set res [r HSETEX myhash XX FIELDS 2 f1 new1 f2 new2]
    #    assert_equal 1 $res
    #    assert_equal new1 [r HGET myhash f1]
    #    assert_equal new2 [r HGET myhash f2]
    #}

    #test {HSETEX XX - non-existing key blocked} {
    #    r FLUSHALL
    #    set res [r HSETEX myhash XX FIELDS 2 f1 v1 f2 v2]
    #    assert_equal 0 $res
    #    assert_equal 0 [r EXISTS myhash]
    #    assert_equal 0 [r HEXISTS myhash f1]
    #    assert_equal 0 [r HEXISTS myhash f2]
    #}

    ## FNX/FXX

    # hsetex throws ERR *, it shouldn't
    test {HSETEX EX FNX - set only if none of the fields exist} {
        r FLUSHALL
        r HSET myhash field1 val1
        set res [r HSETEX myhash EX 10 FNX FIELDS 1 field1 val2]
        assert_equal 0 $res
        assert_equal val1 [r HGET myhash field1]

        # Now try with all-new fields
        set res [r HSETEX myhash EX 10 FNX FIELDS 2 f2 v2 f3 v3]
        assert_equal 1 $res
        assert_equal v2 [r HGET myhash f2]
        assert_equal v3 [r HGET myhash f3]
    }

    test {HSETEX EX FXX - set only if all fields exist} {
        r FLUSHALL
        r HSET myhash field1 val1 field2 val2
        set res [r HSETEX myhash EX 10 FXX FIELDS 2 field1 new1 field2 new2]
        assert_equal 1 $res
        assert_equal new1 [r HGET myhash field1]
        assert_equal new2 [r HGET myhash field2]

        # Now try when one field doesn't exist
        set res [r HSETEX myhash EX 10 FXX FIELDS 2 field1 x fieldX y]
        assert_equal 0 $res
        assert_equal new1 [r HGET myhash field1]
        assert_equal 0 [r HEXISTS myhash fieldX]
    }

    # Syntax error: HSETEX myhash PX 100 FNX FIELDS 2 x 2 y 3
    test {HSETEX PX FNX - partial conflict returns 0} {
        r FLUSHALL
        r HSET myhash x 1
        set res [r HSETEX myhash PX 100 FNX FIELDS 2 x 2 y 3]
        assert_equal 0 $res
        assert_equal 1 [r HEXISTS myhash x]
        assert_equal 0 [r HEXISTS myhash y]
    }

    test {HSETEX PX FXX - one field missing returns 0} {
        r FLUSHALL
        r HSET myhash a 1
        set res [r HSETEX myhash PX 100 FXX FIELDS 2 a 2 b 3]
        assert_equal 0 $res
        assert_equal 1 [r HGET myhash a]
        assert_equal 0 [r HEXISTS myhash b]
    }

    test {HSETEX EX - FNX and FXX conflict error} {
        catch {r HSETEX myhash EX 10 FNX FXX FIELDS 1 x y} e
        set e
    } {ERR *}

     test {HSETEX EX - FXX does not create object in case key does not exist} {
        r FLUSHALL
        assert_equal 0 [r HSETEX myhash EX 10 FXX FIELDS 1 x y]
        assert_equal 0 [r EXISTS myhash]
    }

    ## NX/XX + FNX/FXX combinations

    # NX + FNX — only set if key does not exist AND fields do not exist
    #test {HSETEX EX NX FNX - set only if key missing and fields missing} {
    #    r FLUSHALL
    #    set res [r HSETEX myhash EX 10 NX FNX FIELDS 2 f1 v1 f2 v2]
    #    assert_equal 1 $res
    #    assert_equal v1 [r HGET myhash f1]
    #    assert_equal v2 [r HGET myhash f2]
#
    #    # Try again — key exists now, should block
    #    set res [r HSETEX myhash EX 10 NX FNX FIELDS 2 f3 v3 f4 v4]
    #    assert_equal 0 $res
    #    assert_equal 0 [r HEXISTS myhash f3]
    #    assert_equal 0 [r HEXISTS myhash f4]
    #}

    # NX + FXX — only set if key does not exist AND all fields exist (key missing → blocked)
    #test {HSETEX EX NX FXX - key missing blocks all} {
    #    r FLUSHALL
    #    set res [r HSETEX myhash EX 10 NX FXX FIELDS 2 f1 v1 f2 v2]
    #    assert_equal 0 $res
    #    assert_equal 0 [r EXISTS myhash]
    #    assert_equal 0 [r HEXISTS myhash f1]
    #    assert_equal 0 [r HEXISTS myhash f2]
    #}

    # XX + FNX — only set if key exists AND none of the fields exist
    #test {HSETEX EX XX FNX - set only if key exists and fields missing} {
    #    r FLUSHALL
    #    r HSET myhash f1 old1
    #    set res [r HSETEX myhash EX 10 XX FNX FIELDS 2 f2 v2 f3 v3]
    #    assert_equal 1 $res
    #    assert_equal v2 [r HGET myhash f2]
    #    assert_equal v3 [r HGET myhash f3]
#
    #    # Try again — fields already exist → should block
    #    set res [r HSETEX myhash EX 10 XX FNX FIELDS 2 f2 x f4 y]
    #    assert_equal 0 $res
    #    assert_equal v2 [r HGET myhash f2]
    #    assert_equal 0 [r HEXISTS myhash f4]
    #}

    # XX + FXX — only set if key exists AND all fields exist
    #test {HSETEX EX XX FXX - set only if key exists and all fields exist} {
    #    r FLUSHALL
    #    r HSET myhash f1 old1 f2 old2
    #    set res [r HSETEX myhash EX 10 XX FXX FIELDS 2 f1 new1 f2 new2]
    #    assert_equal 1 $res
    #    assert_equal new1 [r HGET myhash f1]
    #    assert_equal new2 [r HGET myhash f2]
#
    #    # Try when one field is missing → should block
    #    set res [r HSETEX myhash EX 10 XX FXX FIELDS 2 f1 x f3 y]
    #    assert_equal 0 $res
    #    assert_equal new1 [r HGET myhash f1]
    #    assert_equal 0 [r HEXISTS myhash f3]
    #}

    #test {HSETEX is not replicating validation arguments} {
    #    r flushall
    #    set repl [attach_to_replication_stream]
    #    set exp [get_longer_then_long_expire_value PXAT]
#
    #    r HSETEX myhash NX PXAT $exp FIELDS 1 f1 v1
    #    r HSETEX myhash XX PXAT $exp FIELDS 1 f1 v1
    #    r HSETEX myhash FNX PXAT $exp FIELDS 1 f2 v2
    #    r HSETEX myhash FXX PXAT $exp FIELDS 1 f2 v2
    #    r HSETEX myhash2 nx PXAT $exp FIELDS 1 f1 v1
    #    r HSETEX myhash2 xx PXAT $exp FIELDS 1 f1 v1
    #    r HSETEX myhash2 fnx PXAT $exp FIELDS 1 f2 v2
    #    r HSETEX myhash2 fxx PXAT $exp FIELDS 1 f2 v2
#
    #    assert_replication_stream $repl [subst {
    #        {select *}
    #        {hsetex myhash PXAT $exp FIELDS 1 f1 v1}
    #        {hsetex myhash PXAT $exp FIELDS 1 f1 v1}
    #        {hsetex myhash PXAT $exp FIELDS 1 f2 v2}
    #        {hsetex myhash PXAT $exp FIELDS 1 f2 v2}
    #        {hsetex myhash2 PXAT $exp FIELDS 1 f1 v1}
    #        {hsetex myhash2 PXAT $exp FIELDS 1 f1 v1}
    #        {hsetex myhash2 PXAT $exp FIELDS 1 f2 v2}
    #        {hsetex myhash2 PXAT $exp FIELDS 1 f2 v2}
    #    }]
    #    close_replication_stream $repl
    #}
}
