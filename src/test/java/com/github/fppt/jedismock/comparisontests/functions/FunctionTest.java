package com.github.fppt.jedismock.comparisontests.functions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.comparisontests.TestErrorMessages;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.args.FlushMode;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.fppt.jedismock.comparisontests.functions.FunctionUtils.getFunctionCode;
import static com.github.fppt.jedismock.comparisontests.functions.FunctionUtils.getSingleFunctionLibraryCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

@ExtendWith(ComparisonBase.class)
public class FunctionTest {

    @BeforeEach
    void flush(Jedis jedis) {
        jedis.functionFlush();
    }

    @TestTemplate
    void loadWithUnknownArgument(Jedis jedis) {
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.FUNCTION,
                        "LOAD",
                        "foo",
                        "bar",
                        getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Unknown option given: foo");
    }

    @TestTemplate
    void rejectsLoadingAnAlreadyExistingLibrary(Jedis jedis) {
        String libraryName = "test";
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", libraryName, "fun1", "return 'hello'"));
        assertThatThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", libraryName, "fun2", "return 'hello'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(String.format("ERR Library '%s' already exists", libraryName));

        // Library name is case-sensitive
        assertThatNoException().isThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "TEST", "fun3", "return 'hello'")));
        assertThatNoException().isThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "TeSt", "fun4", "return 'hello'")));
    }

    @TestTemplate
    void rejectsLoadingLibraryWithBadFormatName(Jedis jedis) {
        assertThatNoException().isThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "GOOD_f0rmat", "test", "return 'hello'")));

        assertThat(List.of(
                "bad\\0format",
                "bad\1format",
                "bad-format",
                "bad!format",
                "\"bad'format\"",
                "\"bad\\\"format\"",
                "'bad\"format'",
                "'bad\\'format'",
                ""
        )).allSatisfy(libraryName ->
                assertThatThrownBy(() ->
                        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", libraryName, "test", "return 'hello'")))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long")
        );
    }

    @TestTemplate
    void rejectsLoadingLibraryWithMissingMetadata(Jedis jedis) {
        assertThat(List.of(
                "",
                "not a shebang at all",
                "\n"
        )).allSatisfy(malformed ->
                assertThatThrownBy(() -> jedis.functionLoad(malformed))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Missing library metadata")
        );
    }

    @TestTemplate
    void rejectsLoadingLibraryWithUnexistingEngine(Jedis jedis) {
        assertThatNoException().isThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test1", "fun1", "return 'hello'")));
        assertThatNoException().isThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("lua", "test2", "fun2", "return 'hello'")));

        assertThatThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("bad_engine", "test", "test", "return 'hello'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Engine 'bad_engine' not found");
    }

    @TestTemplate
    void rejectsLoadingLibraryWithUncompiledScript(Jedis jedis) {
        assertThatThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "bad script")))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("ERR Error compiling function: user_function:3: ");
    }

    @TestTemplate
    void libraryCanBeReplaced(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello1'"));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello1");

        jedis.functionLoadReplace(getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello2'"));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello2");
    }

    @TestTemplate
    void replaceRemovesFunctionsDroppedFromNewVersion(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'") +
                        getFunctionCode("fun2", "return 'v2'")
        );
        assertThat(jedis.fcall("fun1", List.of(), List.of())).isEqualTo("v1");
        assertThat(jedis.fcall("fun2", List.of(), List.of())).isEqualTo("v2");

        jedis.functionLoadReplace(getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'"));

        assertThat(jedis.functionList()).singleElement().satisfies(libraryInfo ->
                assertThat(libraryInfo.getFunctions()).singleElement().asInstanceOf(MAP)
                        .containsEntry("name", "fun1"));
        assertThatThrownBy(() ->
                jedis.fcall("fun2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }

    @TestTemplate
    void replaceRemovesFunctionsWithMixedCaseNamesDroppedFromNewVersion(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'") +
                        getFunctionCode("Fun2", "return 'v2'")
        );
        assertThat(jedis.fcall("fun1", List.of(), List.of())).isEqualTo("v1");
        assertThat(jedis.fcall("fun2", List.of(), List.of())).isEqualTo("v2");

        jedis.functionLoadReplace(getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'"));

        assertThat(jedis.functionList()).singleElement().satisfies(libraryInfo ->
                assertThat(libraryInfo.getFunctions()).singleElement().asInstanceOf(MAP)
                        .containsEntry("name", "fun1"));
        assertThatThrownBy(() ->
                jedis.fcall("fun2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }

    @TestTemplate
    void replaceRemovesAllFunctionsWhenNewVersionSharesNoFunctionNames(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'") +
                        getFunctionCode("fun2", "return 'v2'")
        );
        assertThat(jedis.fcall("fun1", List.of(), List.of())).isEqualTo("v1");
        assertThat(jedis.fcall("fun2", List.of(), List.of())).isEqualTo("v2");

        jedis.functionLoadReplace(getSingleFunctionLibraryCode("LUA", "test", "fun3", "return 'v3'"));

        assertThat(jedis.functionList()).singleElement().satisfies(libraryInfo ->
                assertThat(libraryInfo.getFunctions()).singleElement().asInstanceOf(MAP)
                        .containsEntry("name", "fun3"));
        assertThatThrownBy(() ->
                jedis.fcall("fun1", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
        assertThatThrownBy(() ->
                jedis.fcall("fun2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
        assertThat(jedis.fcall("fun3", List.of(), List.of())).isEqualTo("v3");
    }

    @TestTemplate
    void replaceKeepsFunctionWhoseNameOnlyChangedCase(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'") +
                        getFunctionCode("Fun2", "return 'v2'")
        );
        assertThat(jedis.fcall("fun2", List.of(), List.of())).isEqualTo("v2");

        jedis.functionLoadReplace(
                getSingleFunctionLibraryCode("LUA", "test", "fun1", "return 'v1'") +
                        getFunctionCode("fun2", "return 'v3'")
        );

        assertThat(jedis.fcall("fun2", List.of(), List.of())).isEqualTo("v3");
    }

    @TestTemplate
    void librariesCanBeFlushed(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", "hi", "return 'hello1'") +
                        getFunctionCode("bye", "return 'bye'")
        );
        assertThat(jedis.fcall("hi", List.of(), List.of())).isEqualTo("hello1");
        assertThat(jedis.fcall("bye", List.of(), List.of())).isEqualTo("bye");

        assertThatThrownBy(() ->
                jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "hi", "return 'hello2'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Library 'test' already exists");
        assertThat(jedis.fcall("hi", List.of(), List.of())).isEqualTo("hello1");

        assertThat(jedis.functionFlush()).isEqualTo("OK");
        assertThatThrownBy(() ->
                jedis.fcall("hi", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
        assertThatThrownBy(() ->
                jedis.fcall("bye", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");

        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "hi", "return 'hello2'"));
        assertThat(jedis.fcall("hi", List.of(), List.of())).isEqualTo("hello2");
    }

    @TestTemplate
    void librariesCanBeDeleted(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "lib1", "hi1", "return 'hello1'") +
                        getFunctionCode("hi2", "return 'hello2'")
        );
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib2", "bye", "return 'bye'"));
        assertThat(jedis.fcall("hi1", List.of(), List.of())).isEqualTo("hello1");
        assertThat(jedis.fcall("hi2", List.of(), List.of())).isEqualTo("hello2");
        assertThat(jedis.fcall("bye", List.of(), List.of())).isEqualTo("bye");

        assertThat(jedis.functionDelete("lib1")).isEqualTo("OK");
        assertThat(jedis.functionList()).singleElement()
                .satisfies(libraryInfo -> assertThat(libraryInfo.getLibraryName()).isEqualTo("lib2"));
        assertThatThrownBy(() ->
                jedis.fcall("hi1", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
        assertThatThrownBy(() ->
                jedis.fcall("hi2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
        assertThat(jedis.fcall("bye", List.of(), List.of())).isEqualTo("bye");

        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib1", "hi", "return 'hello3'"));
        assertThat(jedis.fcall("hi", List.of(), List.of())).isEqualTo("hello3");
    }

    @TestTemplate
    void deletingLibraryRemovesFunctionsWithMixedCaseNames(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib1", "Hi1", "return 'hello1'"));
        assertThat(jedis.fcall("hi1", List.of(), List.of())).isEqualTo("hello1");

        assertThat(jedis.functionDelete("lib1")).isEqualTo("OK");

        assertThatThrownBy(() ->
                jedis.fcall("hi1", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }

    @TestTemplate
    void rejectsDeletingNonExistentLibrary(Jedis jedis) {
        assertThatThrownBy(() ->
                jedis.functionDelete("does_not_exist"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Library not found");
    }

    @TestTemplate
    void rejectsFunctionKillWhenNoFunctionIsRunning(Jedis jedis) {
        assertThatThrownBy(jedis::functionKill)
                .isInstanceOf(JedisDataException.class)
                .hasMessage("NOTBUSY No scripts in execution right now.");
    }

    @TestTemplate
    void rejectsBadSubcommand(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FUNCTION, "bad_subcommand"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR unknown subcommand 'bad_subcommand'. Try FUNCTION HELP.");
    }

    @TestTemplate
    void flushAllAndFlushDbDoNotFlushFunctions(Jedis jedis) {
        assertThat(jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello'"))).isEqualTo("test");
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello");

        jedis.flushAll();
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello");

        jedis.flushDB();
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello");
    }

    @TestTemplate
    void killCommandStopsRunningFunction(Jedis jedis, HostAndPort hostAndPort) throws InterruptedException {
        jedis.configSet("busy-reply-threshold", "10");
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib", "test", "local a = 1 while true do a = a + 1 end"));

        AtomicReference<Exception> caughtException = new AtomicReference<>();
        CountDownLatch exceptionGate = new CountDownLatch(1);
        Jedis busyClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> {
                try {
                    busyClient.fcall("test", List.of(), List.of());
                } catch (Exception e) {
                    caughtException.set(e);
                    exceptionGate.countDown();
                }
            });

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                Thread.sleep(100);
                assertThatThrownBy(jedis::ping)
                        .isInstanceOf(JedisBusyException.class)
                        .hasMessage("BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.");
                jedis.functionKill();
                assertThat(jedis.ping()).isEqualTo("PONG");
            }, TestErrorMessages.DEADLOCK_ERROR_MESSAGE);
        } finally {
            pool.shutdownNow();
            busyClient.close();
        }

        assertThat(exceptionGate.await(5, TimeUnit.SECONDS)).withFailMessage("fcall did not throw an exception").isTrue();
        assertThat(caughtException.get())
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Script killed by user with SCRIPT KILL... script: test, on @user_function:3.");
    }

    @TestTemplate
    void registeredFunctionsCanBeListed(Jedis jedis) {
        assertThat(jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "hi", "return 'hello'"))).isEqualTo("test");
        assertThat(jedis.functionList()).singleElement().satisfies(libraryInfo -> {
            assertThat(libraryInfo.getEngine()).isEqualTo("LUA");
            assertThat(libraryInfo.getLibraryName()).isEqualTo("test");
            assertThat(libraryInfo.getFunctions()).singleElement().asInstanceOf(MAP)
                    .containsOnly(entry("name", "hi"), entry("description", null), entry("flags", List.of()));
            assertThat(libraryInfo.getLibraryCode()).isNull();
        });
    }

    @TestTemplate
    void functionStatsCanBeRetrieved(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "lib1", "hi", "return 'hello'")
                        + getFunctionCode("bye", "return 'bye'")
        );

        assertThat(jedis.functionStats()).satisfies(stats -> {
            assertThat(stats.getEngines()).hasSize(1).hasEntrySatisfying("LUA", engine ->
                    assertThat(engine).containsOnly(entry("libraries_count", 1L), entry("functions_count", 2L)));
            assertThat(stats.getRunningScript()).isNull();
        });

        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib2", "hi2", "return 'hello2'"));

        assertThat(jedis.functionStats()).satisfies(stats ->
                assertThat(stats.getEngines()).hasSize(1).hasEntrySatisfying("LUA", engine ->
                        assertThat(engine).containsOnly(entry("libraries_count", 2L), entry("functions_count", 3L)))
        );
    }

    @TestTemplate
    void functionStatsCanBeRetrievedWhileFunctionIsRunning(Jedis jedis, HostAndPort hostAndPort) {
        jedis.configSet("busy-reply-threshold", "1");
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "local a = 1 while true do a = a + 1 end"));

        Jedis busyClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Instant start = Instant.now();
            pool.submit(() -> busyClient.fcall("test", List.of("my-key"), List.of("my-value")));

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThatThrownBy(jedis::ping)
                            .isInstanceOf(JedisBusyException.class)
                            .hasMessage("BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.")
            );

            assertThat(jedis.functionStats().getRunningScript())
                    .containsOnlyKeys("name", "command", "duration_ms")
                    .containsEntry("name", "test")
                    .containsEntry("command", List.of("FCALL", "test", "1", "my-key", "my-value"))
                    .hasEntrySatisfying("duration_ms", duration ->
                            assertThat(duration).asInstanceOf(LONG)
                                    .isBetween(0L, Duration.between(start, Instant.now()).toMillis()));

            assertThatThrownBy(jedis::ping)
                    .isInstanceOf(JedisBusyException.class)
                    .hasMessage("BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.");
        } finally {
            try {
                jedis.functionKill();
                jedis.ping();
            } finally {
                pool.shutdownNow();
                busyClient.close();
            }
        }
    }

    @TestTemplate
    void rejectsFlushWithBadArgs(Jedis jedis) {
        assertThatNoException().isThrownBy(() -> jedis.functionFlush(FlushMode.SYNC));
        assertThatNoException().isThrownBy(() -> jedis.functionFlush(FlushMode.ASYNC));

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FUNCTION, "flush", "bad_arg"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FUNCTION, "FLUSH", "sync", "extra_arg"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR unknown subcommand or wrong number of arguments for 'FLUSH'. Try FUNCTION HELP.");
    }

    @TestTemplate
    void functionHelpReturnsUsefulInfo(Jedis jedis) {
        CommandArguments commandArgs = new CommandArguments(Protocol.Command.FUNCTION).add(Protocol.Keyword.HELP);
        assertThat(jedis.getConnection().executeCommand(new CommandObject<>(commandArgs, BuilderFactory.STRING_LIST)))
                .startsWith(
                        "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "LOAD [REPLACE] <FUNCTION CODE>",
                        "    Create a new library with the given library name and code.",
                        "DELETE <LIBRARY NAME>",
                        "    Delete the given library.",
                        "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
                        "    Return general information on all the libraries:",
                        "    * Library name",
                        "    * The engine used to run the Library",
                        "    * Functions list",
                        "    * Library code (if WITHCODE is given)",
                        "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
                        "STATS",
                        "    Return information about the current function running:",
                        "    * Function name",
                        "    * Command used to run the function",
                        "    * Duration in MS that the function is running",
                        "    If no function is running, return nil",
                        "    In addition, returns a list of available engines.",
                        "KILL",
                        "    Kill the current running function.",
                        "FLUSH [ASYNC|SYNC]",
                        "    Delete all the libraries.",
                        "    When called without the optional mode argument, the behavior is determined by the",
                        "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
                        "    * ASYNC: Asynchronously flush the libraries.",
                        "    * SYNC: Synchronously flush the libraries."
                );
    }

    @TestTemplate
    void functionRegistrationFailureRevertsTheEntireLoad(Jedis jedis) {
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", "hi1", "return 'hello1'") +
                        getFunctionCode("hi2", "return 'hello2'")
        );
        assertThat(jedis.fcall("hi1", List.of(), List.of())).isEqualTo("hello1");
        assertThat(jedis.fcall("hi2", List.of(), List.of())).isEqualTo("hello2");

        assertThatThrownBy(() -> jedis.functionLoadReplace(
                """
                        #!lua name=test
                        redis.register_function(
                            'hi1',
                            function(keys, args)
                                return 'hello3'
                            end
                        )
                        redis.register_function(
                            'hi2',
                            'not a function'
                        )
                        """
        ))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR second argument to redis.register_function must be a function");

        assertThat(jedis.fcall("hi1", List.of(), List.of())).isEqualTo("hello1");
        assertThat(jedis.fcall("hi2", List.of(), List.of())).isEqualTo("hello2");
    }

    @TestTemplate
    void rejectsFunctionNameCollisions(Jedis jedis) {
        String collidingFunctionName = "hi1";
        jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "test", collidingFunctionName, "return 'hello1'") +
                        getFunctionCode("hi2", "return 'hello2'")
        );

        assertThatThrownBy(() -> jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib2", collidingFunctionName, "return 'hello1'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function hi1 already exists");
        assertThatThrownBy(() -> jedis.functionLoadReplace(getSingleFunctionLibraryCode("LUA", "lib2", collidingFunctionName, "return 'hello1'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function hi1 already exists");

        assertThat(jedis.fcall(collidingFunctionName, List.of(), List.of())).isEqualTo("hello1");
        assertThat(jedis.fcall("hi2", List.of(), List.of())).isEqualTo("hello2");
    }

    @TestTemplate
    void rejectsFunctionNameCollisionsWithinTheLibrary(Jedis jedis) {
        String functionName = "test";
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib1", functionName, "return 'hello1'"));

        assertThatThrownBy(() -> jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "lib2", functionName, "return 'hello2'") +
                        getFunctionCode(functionName, "return 'hello3'")
        ))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR Function already exists in the library");

        assertThat(jedis.fcall(functionName, List.of(), List.of())).isEqualTo("hello1");
    }

    @TestTemplate
    void allowsFunctionNamesDifferingOnlyByCaseWithinTheLibrary(Jedis jedis) {
        // Unlike a literal duplicate name, real Redis does not treat two
        // differently-cased names as a collision at registration time: both
        // are kept (and listed), and the case-insensitive FCALL lookup
        // consistently resolves every case variant to whichever of the two
        // wins in the underlying function table.
        assertThat(jedis.functionLoad(
                getSingleFunctionLibraryCode("LUA", "lib1", "test", "return 'v1'") +
                        getFunctionCode("TEST", "return 'v2'")
        )).isEqualTo("lib1");

        assertThat(jedis.functionList()).singleElement().satisfies(libraryInfo ->
                assertThat(libraryInfo.getFunctions())
                        .extracting(function -> function.get("name"))
                        .containsExactlyInAnyOrder("test", "TEST"));

        Object result = jedis.fcall("test", List.of(), List.of());
        assertThat(List.of("TEST", "TeSt")).allSatisfy(name ->
                assertThat(jedis.fcall(name, List.of(), List.of())).isEqualTo(result));
    }

    @TestTemplate
    void rejectsCallingRegisterFunctionWithInvalidArgs(Jedis jedis) {
        assertThat(Map.of(
                "", "ERR wrong number of arguments to redis.register_function",
                "'f1'", "ERR calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).",
                "'f1', function() return 1 end, {}, 'description'", "ERR wrong number of arguments to redis.register_function",
                "nil, function() return 1 end", "ERR first argument to redis.register_function must be a string",
                "true, function() return 1 end", "ERR first argument to redis.register_function must be a string",
                "{}, function() return 1 end", "ERR first argument to redis.register_function must be a string",
                "1.23, function() return 1 end", "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
                "-1, function() return 1 end", "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
                "'test\0test', function() return 1 end", "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
                "'', function() return 1 end", "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long"
        )).allSatisfy((args, expectedMessage) ->
                assertThatThrownBy(() -> jedis.functionLoad("#!lua name=lib\nredis.register_function(%s)\n".formatted(args)))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Error registering functions: " + expectedMessage)
        );
        assertThat(jedis.functionList()).isEmpty();
    }

    @TestTemplate
    void numbersAreValidFunctionNames(Jedis jedis) {
        assertThat(Map.of(
                "0", "0",
                "1", "1",
                "123", "123",
                // Lua treats whole number decimals the same as their integer counterpart.
                "1.0", "1",
                "0.0", "0"
        )).allSatisfy((registerName, callName) -> {
            jedis.functionFlush();
            assertThatNoException().isThrownBy(() -> {
                jedis.functionLoad("#!lua name=lib\nredis.register_function(%s, function() return 'hello' end)\n".formatted(registerName));
                assertThat(jedis.fcall(callName, List.of(), List.of())).isEqualTo("hello");
            });
        });
    }

    @TestTemplate
    void sharedFunctionCanAccessDefaultGlobals(Jedis jedis) {
        jedis.functionLoad(
                """
                        #!lua name=lib
                        local function ping()
                            return redis.call('ping')
                        end
                        redis.register_function(
                            'f1',
                            function(keys, args)
                                return ping()
                            end
                        )
                        """
        );
        assertThat(jedis.fcall("f1", List.of(), List.of())).isEqualTo("PONG");
    }

    @TestTemplate
    void cannotAccessStandardGlobalsFromLibraryScript(Jedis jedis) {
        assertThatThrownBy(() -> jedis.functionLoad("#!lua name=lib\nreturn math.random()"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR user_function:2: Script attempted to access nonexistent global variable 'math'");
    }

    @TestTemplate
    void cannotCallRedisOperationsFromLibraryScript(Jedis jedis) {
        assertThatThrownBy(() -> jedis.functionLoad("#!lua name=lib\nreturn redis.call('ping')"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR user_function:2: Script attempted to access nonexistent global variable 'call'");
    }

    @TestTemplate
    void rejectsMaliciousAccessAttempt(Jedis jedis) {
        jedis.functionLoad(
                """
                        #!lua name=lib1
                        local lib = redis
                        lib.register_function('f1', function ()
                            lib.redis = redis
                            lib.math = math
                            return {ok='OK'}
                        end)
                        
                        lib.register_function('f2', function ()
                            lib.register_function('f1', function ()
                                lib.redis = redis
                                lib.math = math
                                return {ok='OK'}
                            end)
                        end)
                        """
        );

        assertThatThrownBy(() -> jedis.fcall("f1", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR user_function:4: Attempt to modify a readonly table script: f1, on @user_function:4.");

        assertThatThrownBy(() -> jedis.functionLoad("#!lua name=lib2\nredis.math.random()"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR user_function:2: Script attempted to access nonexistent global variable 'math'");

        assertThatThrownBy(() -> jedis.functionLoad("#!lua name=lib2\nredis.redis.call('ping')"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR user_function:2: Script attempted to access nonexistent global variable 'redis'");

        assertThatThrownBy(() -> jedis.fcall("f2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR redis.register_function can only be called on FUNCTION LOAD command script: f2, on @user_function:10.");
    }

    @TestTemplate
    void rejectsLibraryWithNoFunctions(Jedis jedis) {
        assertThatThrownBy(() -> jedis.functionLoad("#!lua name=lib\nreturn 1"))
                .isInstanceOfAny(JedisDataException.class)
                .hasMessage("ERR No functions registered");
    }

    @TestTemplate
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void rejectsLibraryThatTakesTooLongToExecute(Jedis jedis) {
        assertThatThrownBy(() -> jedis.functionLoad(
                """
                        #!lua name=lib
                        local a = 1
                        while 1 do a = a + 1 end
                        """
        ))
                .isInstanceOfAny(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR FUNCTION LOAD timeout");
    }

    @TestTemplate
    void functionsCanBeRegisteredViaNamedArguments(Jedis jedis) {
        jedis.functionLoad(
                """
                        #!lua name=lib
                        redis.register_function{
                            function_name='f1',
                            callback=function()
                                return 'hello'
                            end,
                            description='some desc'
                        }
                        """
        );
        assertThat(jedis.functionList()).singleElement().satisfies(libraryInfo ->
                assertThat(libraryInfo.getFunctions()).singleElement().asInstanceOf(MAP)
                        .containsOnly(entry("name", "f1"), entry("description", "some desc"), entry("flags", List.of())));
    }

    @TestTemplate
    void rejectsNonStringNamedArgumentKeys(Jedis jedis) {
        assertThatThrownBy(() -> jedis.functionLoad(
                """
                        #!lua name=lib
                        local args = {
                            callback=function()
                                return 'hello'
                            end
                        }
                        local function_name = function()
                            return 'hello'
                        end
                        args[function_name] = 'f1'
                        redis.register_function(args)
                        """
        ))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR named argument key given to redis.register_function is not a string");
    }

    @TestTemplate
    void listWithCodeAndLibraryName(Jedis jedis) {
        jedis.functionLoad("#!lua name=other_lib\nredis.register_function('bye', function(keys, args) return 'bye' end)");
        String code = "#!lua name=my_lib\nredis.register_function('hi', function(keys, args) return 'hello' end)";
        jedis.functionLoad(code);
        assertThat(jedis.functionListWithCode("my_lib")).singleElement().satisfies(libraryInfo -> {
            assertThat(libraryInfo.getEngine()).isEqualTo("LUA");
            assertThat(libraryInfo.getLibraryName()).isEqualTo("my_lib");
            assertThat(libraryInfo.getFunctions()).singleElement().asInstanceOf(MAP)
                    .containsOnly(entry("name", "hi"), entry("description", null), entry("flags", List.of()));
            assertThat(libraryInfo.getLibraryCode()).isEqualTo(code);
        });
    }

    @TestTemplate
    void rejectsInvalidLibraryName(Jedis jedis) {
        assertThat(List.of(
                "name=\"foo",
                "name='foo",
                "name=\"foo'",
                "name='foo\"",
                "name=foo\"",
                "name=foo'",
                "name=fo\"o",
                "name=fo'o"
        )).allSatisfy(nameMetadata ->
                assertThatThrownBy(() -> jedis.functionLoad(
                        """
                                #!lua %s
                                redis.register_function('fun1', function() return 1 end)
                                """.formatted(nameMetadata)
                ))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Invalid library metadata")
        );
    }

    @TestTemplate
    void rejectsInvalidLibraryMetadata(Jedis jedis) {
        assertThat(List.of(
                "#!",
                "#! name=lib",
                "#!lua name=lib",
                "#!lua"
        )).allSatisfy(metadata ->
                assertThatThrownBy(() -> jedis.functionLoad(metadata))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Invalid library metadata")
        );
    }

    @TestTemplate
    void quotedLibraryName(Jedis jedis) {
        assertThat(List.of(
                "name=\"foo\"",
                "name='foo'",
                "name=f\"oo\"",
                "name=f'oo'"
        )).allSatisfy(nameMetadata -> assertThat(jedis.functionLoadReplace(
                """
                        #!lua %s
                        redis.register_function('fun1', function() return 1 end)
                        """.formatted(nameMetadata)
        )).isEqualTo("foo"));
    }

    @TestTemplate
    void preventsRegisteringFunctionsInsideFunctions(Jedis jedis) {
        jedis.functionLoad(
                """
                        #!lua name=lib
                        redis.register_function(
                            'f1',
                            function(keys, args)
                                redis.register_function(
                                    'f2',
                                    function(key, args)
                                        return 2
                                    end
                                )
                                return 1
                            end
                        )
                        """
        );
        assertThatThrownBy(() -> jedis.fcall("f1", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("ERR user_function:5: attempt to call ")
                .hasMessageEndingWith(" script: f1, on @user_function:5.");
        assertThatThrownBy(() -> jedis.fcall("f2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }
}
