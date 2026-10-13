package com.github.fppt.jedismock.storage;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.github.fppt.jedismock.storage.ScriptingManager.KillOutcome;
import static com.github.fppt.jedismock.storage.ScriptingManager.ScriptType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the kill/write transitions of {@link ScriptingManager}. In
 * production the kill comes from another connection while the script thread
 * dispatches commands, so the interesting cases are the two orders in which
 * {@code requestKill} and {@code markWrite} can happen: whichever comes first
 * must win, and the other must be refused.
 */
class ScriptingManagerTest {

    private final ScriptingManager manager = new ScriptingManager();

    @Test
    void killWithNothingRunningIsNotBusy() {
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.NOT_BUSY);
        assertThat(manager.isKillRequested()).isFalse();
    }

    @Test
    void readOnlyScriptIsKilled() {
        manager.start();
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.KILLED);
        assertThat(manager.isKillRequested()).isTrue();
    }

    @Test
    void writeBeforeKillMakesScriptUnkillable() {
        manager.start();
        assertThat(manager.markWrite()).isTrue();
        assertThat(manager.hasWritten()).isTrue();
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.UNKILLABLE);
        assertThat(manager.isKillRequested()).isFalse();
        // A second write (and a second kill attempt) change nothing.
        assertThat(manager.markWrite()).isTrue();
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.UNKILLABLE);
    }

    @Test
    void killBeforeWriteRefusesTheWrite() {
        manager.start();
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.KILLED);
        assertThat(manager.markWrite()).isFalse();
        assertThat(manager.hasWritten()).isFalse();
    }

    @Test
    void writeIsCheckedBeforeScriptTypeMismatch() {
        manager.startFunction("f", "FCALL", Collections.emptyList());
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.WRONG_TYPE);
        assertThat(manager.markWrite()).isTrue();
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.UNKILLABLE);
        assertThat(manager.requestKill(ScriptType.FUNCTION)).isEqualTo(KillOutcome.UNKILLABLE);
    }

    @Test
    void functionKillCannotKillScriptAndViceVersa() {
        manager.start();
        assertThat(manager.requestKill(ScriptType.FUNCTION)).isEqualTo(KillOutcome.WRONG_TYPE);
        assertThat(manager.isKillRequested()).isFalse();
        manager.stop();
        manager.startFunction("f", "FCALL", Collections.emptyList());
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.WRONG_TYPE);
        assertThat(manager.requestKill(ScriptType.FUNCTION)).isEqualTo(KillOutcome.KILLED);
    }

    @Test
    void startAndStopResetWriteAndKillFlags() {
        manager.start();
        manager.markWrite();
        manager.stop();
        assertThat(manager.hasWritten()).isFalse();
        assertThat(manager.isRunning()).isFalse();

        manager.start();
        assertThat(manager.hasWritten()).isFalse();
        assertThat(manager.requestKill(ScriptType.SCRIPT)).isEqualTo(KillOutcome.KILLED);
        manager.stop();
        manager.start();
        assertThat(manager.isKillRequested()).isFalse();
        assertThat(manager.markWrite()).isTrue();
    }

    @Test
    void readOnlyModeIsRecordedPerRunAndClearedOnStop() {
        assertThat(manager.isReadOnly()).isFalse();
        manager.start(true);
        assertThat(manager.isReadOnly()).isTrue();
        manager.stop();
        assertThat(manager.isReadOnly()).isFalse();
        manager.startFunction("f", "FCALL_RO", Collections.emptyList(), true);
        assertThat(manager.isReadOnly()).isTrue();
        manager.stop();
        manager.start();
        assertThat(manager.isReadOnly()).isFalse();
    }
}
