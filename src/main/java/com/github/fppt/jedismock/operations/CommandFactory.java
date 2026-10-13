package com.github.fppt.jedismock.operations;

public class CommandFactory {

    /**
     * Finds the command registered under a name, or {@code null} if there is
     * none. The registry is written at compile time from the
     * {@link RedisCommand} annotations: each command package gets a generated
     * registrar that constructs its operations directly, with the constructor
     * arguments resolved from their declared types by the annotation processor.
     */
    public static CommandDescriptor lookup(String name) {
        return CommandRegistries.commands.get(name);
    }

    /**
     * Forces the eager initialization of the generated registry, so that the
     * first command served does not pay for it, and fails fast if the registry
     * came out empty, which means the annotation processor did not run.
     */
    public static void initialize() {
        if (CommandRegistries.commands.isEmpty()) {
            throw new IllegalStateException("No Redis commands are registered");
        }
    }
}
