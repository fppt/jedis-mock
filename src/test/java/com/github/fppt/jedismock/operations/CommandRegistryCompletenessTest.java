package com.github.fppt.jedismock.operations;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the hand-maintained per-package command registries against drift.
 * Every class carrying {@link RedisCommand} in the sources must be registered,
 * and nothing else may be.
 */
class CommandRegistryCompletenessTest {

    private static final Path OPERATIONS_ROOT = Paths.get(
            System.getProperty("user.dir"),
            "src", "main", "java", "com", "github", "fppt", "jedismock", "operations");

    @Test
    void registriesMatchTheAnnotatedSources() throws IOException {
        Set<String> declared = declaredInSources();
        Set<String> registered = CommandFactory.registeredCommandClasses().stream()
                .map(Class::getName)
                .collect(Collectors.toSet());

        assertThat(registered)
                .as("The per-package command registries are out of sync with the "
                        + "@RedisCommand annotations. Re-run "
                        + "scripts/generate-command-registries.sh — it regenerates every "
                        + "<Package>Commands class and rewrites CommandFactory's import "
                        + "block and registration list to match, including for a command "
                        + "in a brand-new subpackage. (For a command in an existing "
                        + "package, you may instead add the class by hand to that "
                        + "package's <Package>Commands.commands() list.)")
                .containsExactlyInAnyOrderElementsOf(declared);
    }

    @Test
    void everyRegisteredCommandHasAUniqueName() {
        Set<Class<? extends RedisOperation>> classes = CommandFactory.registeredCommandClasses();
        Set<String> names = classes.stream()
                .map(c -> c.getAnnotation(RedisCommand.class).value())
                .collect(Collectors.toSet());

        assertThat(names)
                .as("Two operation classes claim the same command name")
                .hasSameSizeAs(classes);
    }

    private static Set<String> declaredInSources() throws IOException {
        try (Stream<Path> paths = Files.walk(OPERATIONS_ROOT)) {
            return paths
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(CommandRegistryCompletenessTest::isAnnotated)
                    .map(CommandRegistryCompletenessTest::toClassName)
                    .collect(Collectors.toSet());
        }
    }

    private static boolean isAnnotated(Path source) {
        try {
            return Files.readAllLines(source, StandardCharsets.UTF_8).stream()
                    .anyMatch(line -> line.startsWith("@RedisCommand"));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + source, e);
        }
    }

    private static String toClassName(Path source) {
        Path relative = OPERATIONS_ROOT.getParent().relativize(source);
        String withoutExtension = relative.toString()
                .substring(0, relative.toString().length() - ".java".length());
        return "com.github.fppt.jedismock."
                + withoutExtension.replace(source.getFileSystem().getSeparator(), ".");
    }
}
