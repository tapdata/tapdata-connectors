package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** AC-18（审计隔离）与 AC-23（字段、限流）的单元部分。 */
class PaimonSpillAuditTest {

    private final List<Object[]> emitted = new ArrayList<>();
    private final AtomicLong clock = new AtomicLong(1_000L);

    @Test
    void eventsCarryProtocolIdentityAndDetails() {
        PaimonSpillAudit audit = new PaimonSpillAudit(this::capture, clock::get,
                name -> "HOSTNAME".equals(name) ? "engine-a" : null);
        IllegalStateException cause = new IllegalStateException("io deletion failed");

        audit.record(PaimonSpillAudit.Event.RECLAIM_FAILED,
                PaimonSpillAudit.details("root", "/data/spill", "lease", "abc-1"), cause);

        assertEquals(1, emitted.size());
        assertSame(PaimonSpillAudit.Event.RECLAIM_FAILED, emitted.get(0)[0]);
        Map<String, Object> fields = fields(0);
        assertEquals("tapdata-paimon-v2", fields.get("protocol"));
        assertEquals(ProcessHandle.current().pid(), fields.get("pid"));
        assertEquals("engine-a", fields.get("host"));
        assertEquals(PaimonJvmIdentity.current(), fields.get("jvmId"));
        assertTrue(String.valueOf(fields.get("classLoader")).contains("@"));
        assertEquals("/data/spill", fields.get("root"));
        assertEquals("abc-1", fields.get("lease"));
        assertSame(cause, emitted.get(0)[2]);
    }

    @Test
    void hostnameIsReadFromEnvironmentOnlyWithoutNameResolution() {
        PaimonSpillAudit audit = new PaimonSpillAudit(this::capture, clock::get, name -> null);
        audit.record(PaimonSpillAudit.Event.LEASE_CREATED, null);
        assertEquals("unknown", fields(0).get("host"));
    }

    @Test
    void rateLimitedEventsAreEmittedOncePerKeyAndWindow() {
        PaimonSpillAudit audit = new PaimonSpillAudit(this::capture, clock::get, name -> null);
        PaimonSpillAudit.Event event = PaimonSpillAudit.Event.ADMISSION_DENIED;

        assertTrue(audit.recordRateLimited(event, "/a", null));
        clock.addAndGet(TimeUnit.MINUTES.toNanos(1));
        assertFalse(audit.recordRateLimited(event, "/a", null), "窗口内同一 key 不重复输出");
        assertTrue(audit.recordRateLimited(event, "/b", null), "不同 key 独立限流");
        assertTrue(audit.recordRateLimited(PaimonSpillAudit.Event.LEGACY_LAYOUT_FOUND, "/a", null),
                "不同事件独立限流");
        clock.addAndGet(PaimonSpillAudit.RATE_LIMIT_NANOS);
        assertTrue(audit.recordRateLimited(event, "/a", null), "窗口过后再次输出");
        assertEquals(4, emitted.size());
    }

    @Test
    void sinkFailuresNeverEscape() {
        PaimonSpillAudit runtimeFailure = new PaimonSpillAudit(
                (event, fields, failure) -> { throw new IllegalStateException("logger down"); }, clock::get, n -> null);
        PaimonSpillAudit linkageFailure = new PaimonSpillAudit(
                (event, fields, failure) -> { throw new NoClassDefFoundError("org/slf4j/Logger"); }, clock::get, n -> null);
        assertDoesNotThrow(() -> runtimeFailure.record(PaimonSpillAudit.Event.LEASE_RETIRED, null));
        assertDoesNotThrow(() -> linkageFailure.recordRateLimited(PaimonSpillAudit.Event.LEASE_RESIDUE, "/a", null));
    }

    @Test
    void defaultAuditWritesThroughTheEngineLoggerName() {
        assertEquals("io.tapdata.connector.paimon.spill", PaimonSpillAudit.LOGGER_NAME);
        assertDoesNotThrow(() -> PaimonSpillAudit.defaultAudit()
                .record(PaimonSpillAudit.Event.LEASE_CREATED, PaimonSpillAudit.details("lease", "test")));
    }

    private void capture(PaimonSpillAudit.Event event, Map<String, Object> fields, Throwable failure) {
        emitted.add(new Object[] {event, fields, failure});
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fields(int index) {
        return (Map<String, Object>) emitted.get(index)[1];
    }
}
