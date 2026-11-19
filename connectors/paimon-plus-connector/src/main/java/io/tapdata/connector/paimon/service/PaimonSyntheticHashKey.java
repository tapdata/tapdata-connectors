package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Persisted wire format for the connector's synthetic primary key. */
final class PaimonSyntheticHashKey {
    static final String VERSION_OPTION = "tapdata.hash-key.encoding-version";
    private static final int WIRE_VERSION = 2;
    static final String VERSION = Integer.toString(WIRE_VERSION);

    private PaimonSyntheticHashKey() {}

    // Tags distinguish logical value types. Variable-size scalars carry a length, containers a
    // count. DataOutput defines the byte order, independent of the engine's default charset/locale.
    // https://docs.oracle.com/javase/8/docs/api/java/io/DataOutput.html
    static String encode(Collection<String> keys, Map<String, Object> data) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(WIRE_VERSION);
            out.writeInt(keys.size());
            for (String key : keys) {
                if (!data.containsKey(key)) {
                    throw new PaimonFatalWriteException("Missing source primary-key field: " + key);
                }
                writeValue(out, data.get(key));
            }
            return DigestUtils.md5Hex(bytes.toByteArray());
        } catch (PaimonFatalWriteException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            // A source object's exception can contain the complete key value.
            throw new PaimonFatalWriteException("Failed to encode synthetic Paimon primary key; causeType="
                    + e.getClass().getSimpleName());
        }
    }

    private static void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte('0');
        } else if (value instanceof CharSequence || value instanceof Character || value instanceof BinaryString) {
            writeText(out, 's', value.toString());
        } else if (value instanceof byte[]) {
            byte[] binary = (byte[]) value;
            out.writeByte('b');
            out.writeInt(binary.length);
            out.write(binary);
        } else if (value instanceof Boolean) {
            out.writeByte('t');
            out.writeBoolean((Boolean) value);
        } else if (value instanceof Number || value instanceof Decimal) {
            BigDecimal number = value instanceof Decimal ? ((Decimal) value).toBigDecimal()
                    : new BigDecimal(value.toString());
            writeText(out, 'n', number.stripTrailingZeros().toString());
        } else if (value instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
            writeTimestamp(out, timestamp.getTime(), timestamp.getNanos() % 1_000_000);
        } else if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            writeTimestamp(out, timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            List<ByteBuffer> entries = new ArrayList<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                entries.add(encodeParts(entry.getKey(), entry.getValue()));
            }
            writeSorted(out, 'm', entries);
        } else if (value instanceof Set) {
            Set<?> set = (Set<?>) value;
            List<ByteBuffer> elements = new ArrayList<>(set.size());
            for (Object element : set) {
                elements.add(encodeParts(element));
            }
            writeSorted(out, 'u', elements);
        } else if (value instanceof Collection) {
            Collection<?> values = (Collection<?>) value;
            out.writeByte('a');
            out.writeInt(values.size());
            for (Object element : values) {
                writeValue(out, element);
            }
        } else if (value.getClass().isArray()) {
            out.writeByte('a');
            int length = Array.getLength(value);
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                writeValue(out, Array.get(value, i));
            }
        } else {
            throw new PaimonFatalWriteException("Unsupported synthetic primary-key value type: "
                    + value.getClass().getName());
        }
    }

    private static void writeText(DataOutputStream out, int tag, String text) throws IOException {
        out.writeByte(tag);
        out.writeInt(text.length());
        out.writeChars(text);
    }

    // Paimon 1.3.2 represents timestamps as milliseconds plus nanoseconds within the millisecond.
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-common/src/main/java/org/apache/paimon/data/Timestamp.java
    private static void writeTimestamp(DataOutputStream out, long millis, int nanosOfMilli) throws IOException {
        out.writeByte('i');
        out.writeLong(millis);
        out.writeInt(nanosOfMilli);
    }

    private static ByteBuffer encodeParts(Object... parts) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            for (Object part : parts) {
                writeValue(out, part);
            }
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static void writeSorted(DataOutputStream out, int tag, List<ByteBuffer> entries) throws IOException {
        entries.sort(ByteBuffer::compareTo);
        out.writeByte(tag);
        out.writeInt(entries.size());
        for (ByteBuffer entry : entries) {
            out.write(entry.array());
        }
    }
}
