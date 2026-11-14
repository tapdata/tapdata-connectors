package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class PaimonDirectoryEvidenceTest {
    @TempDir Path root;

    @Test
    void rootCreationFailureIsAuditedWithOriginalCauseAndFilesystemEvidence() throws Exception {
        Path blocker = Files.write(root.resolve("file"), new byte[]{1});
        AtomicReference<Throwable> cause = new AtomicReference<>();
        AtomicReference<Map<String, Object>> fields = new AtomicReference<>();
        PaimonSpillAudit audit = new PaimonSpillAudit((event, details, failure) -> {
            assertEquals(PaimonSpillAudit.Event.PROVISION_FAILED, event);
            cause.set(failure);
            fields.set(details);
        }, System::nanoTime, k -> null);
        IOException failure = assertThrows(IOException.class, () -> PaimonSpillLease.provision(
                Collections.singletonList(blocker.resolve("spill").toString()), "test.orders",
                PaimonJvmIdentity::current, audit, PaimonSpillLease.NO_HOOK));
        assertSame(failure, cause.get());
        assertTrue(fields.get().get("directoryState").toString().contains("fileStoreType="));
        assertEquals("test.orders", fields.get().get("owner"));
    }

    @Test
    void deniedLockInspectionIsAnOperationalFailureWithCauseRatherThanANormalSkip() throws Exception {
        Path lease = Files.createDirectories(root.resolve(PaimonSpillLease.SUBROOT)
                .resolve(PaimonJvmIdentity.derive(7L, 7L)).resolve("0123456789ab-" + UUID.randomUUID()));
        Files.write(lease.resolve("owner.lock"), new byte[0]);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AccessDeniedException denied = new AccessDeniedException(lease.toString());
        PaimonSpillAudit audit = new PaimonSpillAudit((event, fields, failure) -> {
            if (event == PaimonSpillAudit.Event.RECLAIM_INSPECTION_FAILED) {
                observed.set(failure);
                assertEquals(PaimonSpillAudit.Severity.WARN, event.severity);
            }
        }, System::nanoTime, k -> null);
        PaimonSpillReclaimer.Result result = new PaimonSpillReclaimer(PaimonJvmIdentity::current,
                p -> "ext4", p -> { throw denied; }, PaimonSpillFiles.FILES_DELETE, audit, 64)
                .reclaim(Collections.singletonList(root.toString()));
        assertEquals(1, result.failed);
        assertEquals(0, result.skipped);
        assertSame(denied, observed.get());
        assertTrue(Files.exists(lease.resolve("owner.lock")));
    }

    @Test
    void nonexistentDirectoryReportsNearestExistingFilesystemWithoutCreatingIt() {
        Path missing = root.resolve("missing/spill");
        Map<String, Object> evidence = PaimonLocalDirectories.describe(missing);
        assertEquals(false, evidence.get("exists"));
        assertTrue(evidence.containsKey("usableBytes"));
        assertTrue(evidence.containsKey("readOnly"));
        assertTrue(evidence.containsKey("fileStoreType"));
        assertFalse(Files.exists(missing));
    }
}
