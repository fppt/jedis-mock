package com.github.fppt.jedismock.operations;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that every implemented command reached {@link CommandFactory}.
 *
 * <p>The registries {@code CommandFactory} reads are written at compile time by
 * {@code CommandRegistryProcessor} from the {@code @RedisCommand} annotations, so they
 * cannot drift the way a hand-maintained list can. What they can still miss is a command
 * javac never showed the processor — if the annotation processor path is misconfigured,
 * or if a build recompiles only the sources that changed rather than all of them, the
 * registries are regenerated from a subset. This test reads the annotations straight out
 * of the source tree, which is the one view of "every command" that does not depend on
 * the build having been wired up correctly.
 */
class CommandRegistryCompletenessTest {

    private static final Path SOURCES =
            Paths.get(System.getProperty("user.dir"), "src", "main", "java");

    //Anchored to the start of a line so that a "{@code @RedisCommand("X")}" in a javadoc
    //comment is not mistaken for a command declaration.
    private static final Pattern REDIS_COMMAND =
            Pattern.compile("(?m)^\\s*@RedisCommand\\(\\s*(?:value\\s*=\\s*)?\"([^\"]+)\"");

    @Test
    void everyImplementedCommandIsRegistered() {
        assertThat(registeredCommandNames())
                .containsExactlyInAnyOrderElementsOf(declaredCommandNames());
    }

    private Set<String> registeredCommandNames() {
        return CommandFactory.registeredCommandClasses().stream()
                .map(command -> command.getAnnotation(RedisCommand.class).value())
                .collect(Collectors.toSet());
    }

    private Set<String> declaredCommandNames() {
        try (Stream<Path> sources = Files.walk(SOURCES)) {
            return sources
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .flatMap(CommandRegistryCompletenessTest::commandNamesIn)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + SOURCES, e);
        }
    }

    private static Stream<String> commandNamesIn(Path source) {
        String text;
        try {
            text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + source, e);
        }
        Stream.Builder<String> names = Stream.builder();
        Matcher matcher = REDIS_COMMAND.matcher(text);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names.build();
    }
}
