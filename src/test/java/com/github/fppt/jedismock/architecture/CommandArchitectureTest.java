package com.github.fppt.jedismock.architecture;

import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClass;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Structural rules for the command layer.
 *
 * <p>Commands are found by annotation and instantiated reflectively, so most of what
 * holds the layer together is convention rather than something the compiler checks.
 * These rules make that convention explicit and enforceable.
 *
 * <p>Some of them restate checks {@code CommandRegistryProcessor} already makes at
 * compile time. That is deliberate: the rules are the readable statement of the
 * contract, and they survive a change of generator.
 */
class CommandArchitectureTest {

    private static final String ROOT = "com.github.fppt.jedismock";
    private static final String OPERATIONS = ROOT + ".operations";

    /**
     * The types {@code CommandFactory.buildOperation} knows how to supply. A constructor
     * parameter of any other type is an {@code IllegalArgumentException} the first time
     * the command is invoked, which is a poor way to find out.
     */
    private static final Set<String> INJECTABLE = new HashSet<>(Arrays.asList(
            List.class.getName(),
            OperationExecutorState.class.getName(),
            RedisBase.class.getName(),
            SubscriptionRegistry.class.getName(),
            RedisClient.class.getName()));

    private final JavaClasses mainClasses = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(ROOT);

    // ---------------------------------------------------------------- the annotation

    @Test
    void commandsImplementRedisOperation() {
        classes().that().areAnnotatedWith(RedisCommand.class)
                .should().beAssignableTo(RedisOperation.class)
                .because("CommandFactory instantiates every registered class as a RedisOperation")
                .check(mainClasses);
    }

    @Test
    void commandsLiveInTheOperationsPackage() {
        classes().that().areAnnotatedWith(RedisCommand.class)
                .should().resideInAPackage(OPERATIONS + "..")
                .because("the registry generator only looks there")
                .check(mainClasses);
    }

    @Test
    void commandsAreConcrete() {
        noClasses().that().areAnnotatedWith(RedisCommand.class)
                .should().haveModifier(JavaModifier.ABSTRACT)
                .because("an abstract class cannot be instantiated to serve a command")
                .check(mainClasses);
    }

    @Test
    void commandNamesAreLowerCase() {
        classes().that().areAnnotatedWith(RedisCommand.class)
                .should(haveALowerCaseCommandName())
                .because("command names are matched against the wire protocol after lower-casing")
                .check(mainClasses);
    }

    /**
     * The converse of {@link #commandsImplementRedisOperation()}, and the one rule here
     * that the annotation processor structurally cannot make: it only ever sees classes
     * that already carry the annotation, so a command written without one is invisible
     * to it and silently unreachable at runtime.
     */
    @Test
    void everyOperationIsAnnotated() {
        classes().that().areAssignableTo(RedisOperation.class)
                .and().areNotInterfaces()
                .and().doNotHaveModifier(JavaModifier.ABSTRACT)
                .should().beAnnotatedWith(RedisCommand.class)
                .because("an unannotated operation is never registered and can never be called")
                .check(mainClasses);
    }

    // ------------------------------------------------------- the injection contract

    @Test
    void commandsDeclareExactlyOneConstructor() {
        classes().that().areAnnotatedWith(RedisCommand.class)
                .should(haveExactlyOneConstructor())
                .because("CommandFactory picks getDeclaredConstructors()[0], and the JVM "
                        + "does not promise an order")
                .check(mainClasses);
    }

    @Test
    void commandConstructorsTakeOnlyInjectableTypes() {
        classes().that().areAnnotatedWith(RedisCommand.class)
                .should(takeOnlyInjectableConstructorParameters())
                .because("CommandFactory resolves constructor arguments by type and fails at "
                        + "runtime on anything else")
                .check(mainClasses);
    }

    // ------------------------------------------------------- the abstraction barrier

    /**
     * The execution machinery — {@code CommandFactory}, the executor, the server, the
     * storage layer — reaches commands only through {@link RedisOperation} and the
     * generated registry, which names them as strings. Nothing outside a command
     * package may name a command type.
     *
     * <p>Commands inside those packages may reuse each other, and do: {@code SScan}
     * extends {@code Scan}, {@code LuaRedisCallback} drives {@code Select}.
     */
    @Test
    void onlyCommandPackagesKnowAboutConcreteCommands() {
        noClasses().that(resideOutsideOfCommandPackages())
                .should().dependOnClassesThat().areAnnotatedWith(RedisCommand.class)
                .because("the execution machinery must stay ignorant of individual commands")
                .check(mainClasses);
    }

    /**
     * {@code MockExecutor} is the default {@code RedisCommandInterceptor} — machinery, not
     * a command — but it lives in a command package, so the barrier above exempts it. It
     * cannot simply be moved: it is public API, and the README shows interceptors
     * delegating to {@code MockExecutor.proceed}. So it is named here instead.
     *
     */
    @Test
    void theDefaultInterceptorKnowsNoConcreteCommands() {
        noClass(MockExecutor.class)
                .should().dependOnClassesThat().areAnnotatedWith(RedisCommand.class)
                .because("dispatch goes through CommandFactory, never a named command")
                .check(mainClasses);
    }

    // ------------------------------------------------------------------- conditions

    private static DescribedPredicate<JavaClass> resideOutsideOfCommandPackages() {
        //A command package is a *sub*package of operations; CommandFactory and the
        //RedisOperation API sit directly in operations and are held to the barrier.
        return new DescribedPredicate<>(
                "reside outside of " + OPERATIONS + ".<subpackage>") {
            @Override
            public boolean test(JavaClass javaClass) {
                return !javaClass.getPackageName().startsWith(OPERATIONS + ".");
            }
        };
    }

    private static ArchCondition<JavaClass> haveALowerCaseCommandName() {
        return new ArchCondition<>("have a lower-case command name") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                String name = item.getAnnotationOfType(RedisCommand.class).value();
                boolean ok = !name.trim().isEmpty() && name.equals(name.toLowerCase(Locale.ROOT));
                events.add(new SimpleConditionEvent(item, ok,
                        String.format("%s declares command name '%s'", item.getName(), name)));
            }
        };
    }

    private static ArchCondition<JavaClass> haveExactlyOneConstructor() {
        return new ArchCondition<>("declare exactly one constructor") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                int count = item.getConstructors().size();
                events.add(new SimpleConditionEvent(item, count == 1,
                        String.format("%s declares %d constructors", item.getName(), count)));
            }
        };
    }

    private static ArchCondition<JavaClass> takeOnlyInjectableConstructorParameters() {
        return new ArchCondition<>("take only injectable constructor parameters") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                for (JavaConstructor constructor : item.getConstructors()) {
                    List<String> offending = constructor.getRawParameterTypes().stream()
                            .map(JavaClass::getFullName)
                            .filter(type -> !INJECTABLE.contains(type))
                            .collect(Collectors.toList());
                    events.add(new SimpleConditionEvent(item, offending.isEmpty(),
                            String.format("%s takes non-injectable constructor parameters %s",
                                    item.getName(), offending)));
                }
            }
        };
    }
}
