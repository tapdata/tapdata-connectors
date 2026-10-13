package io.tapdata.kafka.service;

import io.tapdata.exception.TapPdkConfigEx;
import io.tapdata.kafka.data.KafkaOffset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaServiceTest {

    @Nested
    class KafkaOffsetTest {
        @Test
        void shouldKeepKafkaOffsetUnchanged() {
            KafkaOffset offset = new KafkaOffset();
            offset.setOffset("orders", 0, 10L);

            assertSame(offset, KafkaService.normalizeStreamOffset(offset));
        }

        @Test
        void shouldRestoreCheckpointDeserializedAsLinkedHashMap() {
            Map<Object, Object> partitionOffsets = new LinkedHashMap<>();
            partitionOffsets.put("0", 10);
            partitionOffsets.put(1, 20L);
            Map<String, Object> checkpoint = new LinkedHashMap<>();
            checkpoint.put("orders", partitionOffsets);

            KafkaOffset offset = KafkaService.normalizeStreamOffset(checkpoint);

            assertEquals(10L, offset.get("orders").get(0));
            assertEquals(20L, offset.get("orders").get(1));
        }

        @Test
        void shouldRejectInvalidCheckpointStructure() {
            Map<String, Object> checkpoint = new LinkedHashMap<>();
            checkpoint.put("orders", 10L);

            assertThrows(TapPdkConfigEx.class, () -> KafkaService.normalizeStreamOffset(checkpoint));
        }

        @Test
        void shouldRejectInvalidPartition() {
            Map<String, Object> checkpoint = new LinkedHashMap<>();
            checkpoint.put("orders", Map.of("invalid", 10L));

            assertThrows(TapPdkConfigEx.class, () -> KafkaService.normalizeStreamOffset(checkpoint));
        }
    }
}
