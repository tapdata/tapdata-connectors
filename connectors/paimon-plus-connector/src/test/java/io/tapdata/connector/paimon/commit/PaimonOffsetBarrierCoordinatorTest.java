package io.tapdata.connector.paimon.commit;

import io.tapdata.entity.event.TapCallbackOffset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonOffsetBarrierCoordinatorTest {

    @Test
    void heartbeatMustWaitForEveryTableRegardlessOfCommitOrder() {
        PaimonMicroBatchCoordinator coordinator = coordinatorWithTwoTables("source-a");

        assertNull(coordinator.registerHeartbeat("source-a", offset("h1", "source-a")));
        List<PaimonMicroBatchCoordinator.CallbackReservation> afterB =
                coordinator.publishCommit(
                        coordinator.captureCommitTarget("default.b"), 200L);
        assertTrue(afterB.isEmpty());

        List<PaimonMicroBatchCoordinator.CallbackReservation> afterA =
                coordinator.publishCommit(
                        coordinator.captureCommitTarget("default.a"), 201L);

        assertEquals(1, afterA.size());
        assertEquals("h1", afterA.get(0).payload().get(TapCallbackOffset.KEY_STREAM_OFFSET));
        assertEquals(2, afterA.get(0).requiredGenerationByTable().size());
    }

    @Test
    void dependenciesMustOnlyContainTablesThatAcceptedTheLane() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        coordinator.acceptCdc(
                "default.a", 1, Collections.singleton("source-a"), 100L);
        coordinator.acceptCdc(
                "default.b", 1, Collections.singleton("source-b"), 100L);

        assertNull(coordinator.registerHeartbeat("source-a", offset("a", "source-a")));
        List<PaimonMicroBatchCoordinator.CallbackReservation> ready =
                coordinator.publishCommit(
                        coordinator.captureCommitTarget("default.a"), 200L);

        assertEquals(1, ready.size());
        Map<String, Long> required = ready.get(0).requiredGenerationByTable();
        assertEquals(Collections.singletonMap("default.a", 1L), required);
    }

    @Test
    void newerBlockedHeartbeatMustReplaceOlderPendingHeartbeat() {
        PaimonMicroBatchCoordinator coordinator = coordinatorWithTwoTables("source-a");
        assertNull(coordinator.registerHeartbeat("source-a", offset("h1", "source-a")));
        assertNull(coordinator.registerHeartbeat("source-a", offset("h2", "source-a")));

        coordinator.publishCommit(coordinator.captureCommitTarget("default.a"), 200L);
        List<PaimonMicroBatchCoordinator.CallbackReservation> ready =
                coordinator.publishCommit(
                        coordinator.captureCommitTarget("default.b"), 201L);

        assertEquals(1, ready.size());
        assertEquals("h2", ready.get(0).payload().get(TapCallbackOffset.KEY_STREAM_OFFSET));
    }

    @Test
    void reservedHeartbeatMustRemainInFlightWhileNewerHeartbeatStaysPending() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        PaimonMicroBatchCoordinator.CallbackReservation h1 =
                coordinator.registerHeartbeat("source-a", offset("h1", "source-a"));
        PaimonMicroBatchCoordinator.CallbackReservation h2BeforeCompletion =
                coordinator.registerHeartbeat("source-a", offset("h2", "source-a"));

        assertNull(h2BeforeCompletion);
        assertTrue(coordinator.hasInFlight("source-a"));
        assertTrue(coordinator.hasPending("source-a"));

        PaimonMicroBatchCoordinator.CallbackReservation h2 =
                coordinator.completeCallback(h1);

        assertEquals("h2", h2.payload().get(TapCallbackOffset.KEY_STREAM_OFFSET));
        assertNotEquals(h1.token(), h2.token());
        assertTrue(coordinator.hasInFlight("source-a"));
        assertFalse(coordinator.hasPending("source-a"));
    }

    @Test
    void callbackFailureMustRetainInFlightAndLatestPending() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        PaimonMicroBatchCoordinator.CallbackReservation h1 =
                coordinator.registerHeartbeat("source-a", offset("h1", "source-a"));
        coordinator.registerHeartbeat("source-a", offset("h2", "source-a"));

        coordinator.failCallback(h1);

        assertTrue(coordinator.hasInFlight("source-a"));
        assertTrue(coordinator.hasPending("source-a"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void callbackPayloadMustBeDefensivelyCopied() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        List<String> nodeIds = new ArrayList<>(Arrays.asList("source-a", "source-b"));
        TapCallbackOffset input = offset("h1", "source-a");
        input.nodeIds(nodeIds);
        input.put(TapCallbackOffset.KEY_EVENT_TIME, null);

        PaimonMicroBatchCoordinator.CallbackReservation reservation =
                coordinator.registerHeartbeat("source-a", input);
        input.streamOffset("mutated");
        nodeIds.clear();

        TapCallbackOffset output = reservation.payload();
        assertEquals("h1", output.get(TapCallbackOffset.KEY_STREAM_OFFSET));
        assertEquals(Arrays.asList("source-a", "source-b"), output.get(TapCallbackOffset.KEY_NODE_IDS));
        assertTrue(output.containsKey(TapCallbackOffset.KEY_EVENT_TIME));
        assertNull(output.get(TapCallbackOffset.KEY_EVENT_TIME));
        ((List<String>) output.get(TapCallbackOffset.KEY_NODE_IDS)).clear();
        assertEquals(
                Arrays.asList("source-a", "source-b"),
                reservation.payload().get(TapCallbackOffset.KEY_NODE_IDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    void callbackPayloadMustPreserveArrayTypesAndIsolation() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        byte[] bytes = new byte[] {1, 2};
        String[] strings = new String[] {"one", "two"};
        byte[][] nested = new byte[][] {{3, 4}, {5, 6}};
        Map<String, Object> streamOffset = new LinkedHashMap<>();
        streamOffset.put("bytes", bytes);
        streamOffset.put("strings", strings);
        streamOffset.put("nested", nested);

        PaimonMicroBatchCoordinator.CallbackReservation reservation =
                coordinator.registerHeartbeat("source-a", offset(streamOffset, "source-a"));
        bytes[0] = 9;
        strings[0] = "mutated";
        nested[0][0] = 9;

        Map<String, Object> first =
                (Map<String, Object>)
                        reservation.payload().get(TapCallbackOffset.KEY_STREAM_OFFSET);
        assertEquals(byte[].class, first.get("bytes").getClass());
        assertEquals(String[].class, first.get("strings").getClass());
        assertEquals(byte[][].class, first.get("nested").getClass());
        assertArrayEquals(new byte[] {1, 2}, (byte[]) first.get("bytes"));
        assertArrayEquals(new String[] {"one", "two"}, (String[]) first.get("strings"));
        byte[][] firstNested = (byte[][]) first.get("nested");
        assertArrayEquals(new byte[] {3, 4}, firstNested[0]);
        assertArrayEquals(new byte[] {5, 6}, firstNested[1]);

        ((byte[]) first.get("bytes"))[0] = 8;
        ((String[]) first.get("strings"))[0] = "changed-again";
        firstNested[0][0] = 8;

        Map<String, Object> second =
                (Map<String, Object>)
                        reservation.payload().get(TapCallbackOffset.KEY_STREAM_OFFSET);
        assertArrayEquals(new byte[] {1, 2}, (byte[]) second.get("bytes"));
        assertArrayEquals(new String[] {"one", "two"}, (String[]) second.get("strings"));
        assertArrayEquals(new byte[] {3, 4}, ((byte[][]) second.get("nested"))[0]);
    }

    private static PaimonMicroBatchCoordinator coordinatorWithTwoTables(String lane) {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        coordinator.acceptCdc("default.a", 1, Collections.singleton(lane), 100L);
        coordinator.acceptCdc("default.b", 1, Collections.singleton(lane), 100L);
        return coordinator;
    }

    private static TapCallbackOffset offset(Object value, String lane) {
        return new TapCallbackOffset()
                .streamOffset(value)
                .syncStage("CDC")
                .sourceTime(123L)
                .nodeIds(Collections.singletonList(lane));
    }
}
