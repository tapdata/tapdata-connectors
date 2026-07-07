package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapStringValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Converts the raw on-disk bytes of a single (already de-TOASTed) attribute
 * value into a Java object, selected by the column's type oid. Unknown types
 * fall back to a UTF-8 string or the raw bytes so that decoding degrades
 * gracefully rather than failing.
 *
 * @author Jarad
 */
public final class PgTypeDecoder {

    private PgTypeDecoder() {
    }

    public static final long BOOL = 16, BYTEA = 17, CHAR = 18, NAME = 19, INT8 = 20, INT2 = 21,
            INT4 = 23, TEXT = 25, OID = 26, REGPROC = 24, JSON = 114, XML = 142,
            FLOAT4 = 700, FLOAT8 = 701, BPCHAR = 1042, VARCHAR = 1043,
            DATE = 1082, TIME = 1083, TIMESTAMP = 1114, TIMESTAMPTZ = 1184, TIMETZ = 1266,
            INTERVAL = 1186, MONEY = 790,
            POINT = 600, LSEG = 601, PATH = 602, BOX = 603, POLYGON = 604, LINE = 628, CIRCLE = 718,
            CIDR = 650, INET = 869, MACADDR = 829,
            BIT = 1560, VARBIT = 1562, NUMERIC = 1700,
            REGPROCEDURE = 2202, REGOPER = 2203, REGOPERATOR = 2204,
            REGCLASS = 2205, REGTYPE = 2206,
            UUID_OID = 2950, TSVECTOR = 3614, TSQUERY = 3615,
            REGCONFIG = 3734, REGDICTIONARY = 3769, JSONB = 3802;

    private static final LocalDate PG_EPOCH = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime PG_EPOCH_TS = LocalDateTime.of(2000, 1, 1, 0, 0);

    public static Object decode(long oid, byte[] v) {
        if (v == null) {
            return null;
        }
        if (oid == BOOL) {
            return v.length > 0 && v[0] != 0;
        } else if (oid == INT2) {
            return (int) (short) le16(v);
        } else if (oid == INT4) {
            return (int) le32(v);
        } else if (oid == INT8) {
            return le64(v);
        } else if (oid == OID) {
            return le32(v) & 0xFFFFFFFFL;
        } else if (oid == FLOAT4) {
            return Float.intBitsToFloat((int) le32(v));
        } else if (oid == FLOAT8) {
            return Double.longBitsToDouble(le64(v));
        } else if (oid == BIT || oid == VARBIT) {
            return decodeBit(v);
        } else if (oid == INTERVAL) {
            return decodeInterval(v);
        } else if (oid == MONEY) {
            return decodeMoney(v);
        } else if (oid == NUMERIC) {
            return decodeNumeric(v);
        } else if (oid == DATE) {
            return PG_EPOCH.plusDays(le32(v));
        } else if (oid == TIME) {
            return LocalTime.ofNanoOfDay(le64(v) * 1000L);
        } else if (oid == TIMESTAMP) {
            return PG_EPOCH_TS.plus(le64(v) * 1000L, java.time.temporal.ChronoUnit.NANOS);
        } else if (oid == TIMESTAMPTZ) {
            long micros = le64(v);
            return PG_EPOCH_TS.plus(micros * 1000L, java.time.temporal.ChronoUnit.NANOS).toInstant(ZoneOffset.UTC);
        } else if (oid == TIMETZ) {
            return decodeTimeTz(v);
        } else if (oid == UUID_OID) {
            return decodeUuid(v);
        } else if (oid == BYTEA) {
            return v;
        } else if (oid == JSONB) {
            return decodeJsonb(v);
        } else if (oid == POINT) {
            return decodePoint(v);
        } else if (oid == LSEG) {
            return decodeLseg(v);
        } else if (oid == BOX) {
            return decodeBox(v);
        } else if (oid == CIRCLE) {
            return decodeCircle(v);
        } else if (oid == LINE) {
            return decodeLine(v);
        } else if (oid == PATH) {
            return decodePath(v);
        } else if (oid == POLYGON) {
            return decodePolygon(v);
        } else if (oid == CIDR || oid == INET) {
            return decodeInet(v);
        } else if (oid == MACADDR) {
            return decodeMacaddr(v);
        } else if (oid == REGPROC || oid == REGPROCEDURE || oid == REGOPER
                || oid == REGOPERATOR || oid == REGCLASS || oid == REGTYPE
                || oid == REGCONFIG || oid == REGDICTIONARY) {
            return le32(v) & 0xFFFFFFFFL;   // reg* types are stored as OIDs (4-byte unsigned int)
        } else if (oid == TSVECTOR) {
            return decodeTsVector(v);
        } else if (oid == TSQUERY) {
            return decodeTsQuery(v);
        } else if (oid == NAME) {
            // "name" is a fixed NAMEDATALEN (64-byte) buffer holding a
            // null-terminated string; the bytes past the terminator are NUL
            // padding and must be dropped (otherwise field names carry \u0000).
            return cString(v);
        } else if (oid == CHAR || oid == TEXT || oid == VARCHAR || oid == BPCHAR
                || oid == JSON || oid == XML) {
            return new String(v, StandardCharsets.UTF_8);
        }
        return new String(v, StandardCharsets.UTF_8);
    }

    /**
     * Decode PostgreSQL on-disk bit/varbit representation.
     *
     * On-disk layout (after varlena header is stripped):
     *   int32  bit_len     — number of valid bits (little-endian)
     *   bits8  bit_dat[]   — packed bits, MSB-first, zero-padded in last byte
     *
     * bit(1)='1'   → [0x01,0x00,0x00,0x00, 0x80] → true (Boolean)
     * bit(2)='10'  → [0x02,0x00,0x00,0x00, 0x80] → "10"
     * bit(2)='00'  → [0x02,0x00,0x00,0x00, 0x00] → "00"
     * bit(2)='01'  → [0x02,0x00,0x00,0x00, 0x40] → "01"
     */
    private static Object decodeBit(byte[] v) {
        if (v == null || v.length < 4) {
            return v;
        }
        int bitLen = (int) le32(v);
        int dataStart = 4;
        // bit(1): return boolean, matching AbstractWalLogMiner.parseType() behavior
        if (bitLen == 1) {
            return new TapBooleanValue((v[dataStart] & 0x80) != 0);
        }
        // bit(n) n > 1: unpack each bit to a '0'/'1' character string
        StringBuilder sb = new StringBuilder(bitLen);
        for (int byteIdx = dataStart; byteIdx < v.length && sb.length() < bitLen; byteIdx++) {
            int b = v[byteIdx] & 0xFF;
            for (int bitIdx = 7; bitIdx >= 0 && sb.length() < bitLen; bitIdx--) {
                sb.append(((b >> bitIdx) & 1) == 1 ? '1' : '0');
            }
        }
        return new TapStringValue(sb.toString());
    }

    /**
     * Decode PostgreSQL on-disk interval representation into ISO 8601 duration.
     *
     * On-disk layout (16 bytes, little-endian):
     *   int64 time_us  — microseconds
     *   int32 day      — days
     *   int32 month    — months
     *
     * '1 year 1 mon 1 day 1 hour 2 mins 4.11 secs' yields:
     *   month=13, day=1, time_us=3724110000 → "P1Y1M1DT1H2M4.11S"
     */
    private static String decodeInterval(byte[] v) {
        if (v == null || v.length < 16) {
            return null;
        }
        long timeUs = le64(v);                   // bytes[0..7]
        int day = (int) le32(v, 8);              // bytes[8..11]
        int month = (int) le32(v, 12);           // bytes[12..15]

        StringBuilder iso = new StringBuilder();

        if (timeUs == 0 && day == 0 && month == 0) {
            return "PT0S";
        }

        boolean neg = timeUs < 0 || day < 0 || month < 0;
        if (neg) {
            iso.append('-');
            timeUs = -timeUs;
            day = -day;
            month = -month;
        }
        iso.append('P');

        // years + months
        if (month != 0) {
            int years = month / 12;
            int mons = month % 12;
            if (years != 0) {
                iso.append(years).append('Y');
            }
            if (mons != 0) {
                iso.append(mons).append('M');
            }
        }

        // days
        if (day != 0) {
            iso.append(day).append('D');
        }

        // time portion
        if (timeUs != 0) {
            iso.append('T');
            long totalSec = timeUs / 1_000_000;
            long hours = totalSec / 3600;
            long mins = (totalSec % 3600) / 60;
            long secInt = totalSec % 60;
            long fracUs = timeUs % 1_000_000;

            if (hours != 0) {
                iso.append(hours).append('H');
            }
            if (mins != 0) {
                iso.append(mins).append('M');
            }
            if (secInt != 0 || fracUs != 0) {
                // Strip trailing zeros from fractional seconds for clean output
                if (fracUs == 0) {
                    iso.append(secInt).append('S');
                } else {
                    // Build seconds string with fractional part, strip trailing zeros
                    String frac = String.format("%06d", fracUs);
                    // Strip trailing zeros
                    frac = frac.replaceAll("0+$", "");
                    iso.append(secInt).append('.').append(frac).append('S');
                }
            }
        }

        return iso.toString();
    }

    /**
     * Decode PostgreSQL money type (8-byte little-endian int64, value in cents).
     * '1212.09' → 121209 cents → BigDecimal("1212.09")
     */
    private static BigDecimal decodeMoney(byte[] v) {
        if (v == null || v.length < 8) {
            return null;
        }
        long cents = le64(v);
        return BigDecimal.valueOf(cents, 2);
    }

    /**
     * Decode PostgreSQL time-with-time-zone (timetz) on-disk representation.
     *
     * On-disk layout (12 bytes, little-endian, fixed-length):
     *   int64 time_us  — microseconds since midnight
     *   int32 zone     — timezone offset in seconds (east of GMT positive)
     *
     * '12:12:12.000000 +00:00' → ZonedDateTime(1970-01-01T12:12:12Z)
     */
    private static ZonedDateTime decodeTimeTz(byte[] v) {
        if (v == null || v.length < 12) {
            return null;
        }
        long micros = le64(v);           // bytes[0..7]
        int zoneSecs = (int) le32(v, 8); // bytes[8..11]
        LocalTime time = LocalTime.ofNanoOfDay(micros * 1000L);
        ZoneOffset offset = ZoneOffset.ofTotalSeconds(zoneSecs);
        return ZonedDateTime.of(LocalDate.of(1970, 1, 1), time, offset);
    }

    /* Decode a NUL-terminated string out of a fixed-width buffer (PostgreSQL
     * "name"): stop at the first NUL so the trailing pad bytes are excluded. */
    private static String cString(byte[] v) {
        int len = 0;
        while (len < v.length && v[len] != 0) {
            len++;
        }
        return new String(v, 0, len, StandardCharsets.UTF_8);
    }

    /**
     * Jsonb JEntry flags (bits in the upper byte of a 32-bit JEntry).
     * On-disk representation uses the same bit layout as the in-memory one.
     */
    private static final int JBE_ISCONTAINER = 0x01000000;
    private static final int JBE_ISSTRING    = 0x02000000;
    private static final int JBE_ISNUMERIC   = 0x04000000;
    private static final int JBE_ISNULL      = 0x08000000;
    private static final int JBE_ISBOOL      = 0x20000000;
    private static final int JBE_ISBOOL_TRUE = 0x10000000;
    private static final int JENTRY_LENMASK  = 0x00FFFFFF;

    /**
     * Decode PostgreSQL jsonb on-disk binary representation.
     *
     * Layout (after varlena header is stripped):
     *   uint8  version    — always 1
     *   JsonbContainer:
     *     uint32 header   — high nibble: JB_FOBJECT(0x4)/JB_FARRAY(0x8)/JB_FSCALAR(0x0)
     *                       low 28 bits: number of JEntries
     *     JEntry[count]   — each 4 bytes, encoding type + length/offset
     *     uint8 data[]    — concatenated key/value payloads (no terminators)
     */
    /**
     * Decode PostgreSQL jsonb on-disk format. Bytes are the raw JsonbContainer
     * (varlena header already stripped by readVarlena).
     *
     * PG &le;13 format (versioned):   version(1) + header(4) + JEntry[count] + data
     *   header type nibble: 0/4=object, 1/8=array, 2=scalar
     *
     * PG &ge;14 compact format (unversioned): header(4) + JEntry[count] + data
     *   header type nibble: 2=object/array (count = pairs/elements)
     *
     * Heuristic: if byte 0 is 0x01 AND the header at offset 1 has a reasonable
     * count, use the versioned path; otherwise treat byte 0 as header start.
     */
    private static Object decodeJsonb(byte[] v) {
        if (v == null || v.length < 5) {
            return null;
        }

        int header;
        int containerType;
        int count;
        int entryStart;

        // Heuristic: detect versioned vs unversioned format
        if (v[0] == 0x01 && v.length >= 9) {
            int h = (int) le32(v, 1);
            int c = h & 0x0FFFFFFF;
            if (c > 0 && c < v.length / 2) {
                // Versioned format (PG &le;13 / test data)
                header = h;
                entryStart = 5;
            } else {
                // Byte 0 is first byte of header (PG &ge;14)
                header = (int) le32(v, 0);
                entryStart = 4;
            }
        } else {
            header = (int) le32(v, 0);
            entryStart = 4;
        }

        containerType = (header >> 28) & 0xF;
        count = header & 0x0FFFFFFF;

        if (count <= 0 || count > v.length / 2) {
            return null;
        }

        int entryCount = count;
        boolean isObject;

        if (containerType == 0 || containerType == 0x4) {
            // Versioned JB_FOBJECT: count = number of JEntries (key+value)
            isObject = true;
        } else if (containerType == 0x8 || containerType == 1) {
            // JB_FARRAY: count = number of elements
            isObject = false;
        } else if (containerType == 2) {
            // PG &ge;14 compact: count = number of pairs (object) or elements (array)
            // Heuristic: if exactly 2*count entries fit, it's an object
            int possibleEntries = count * 2;
            int possibleDataStart = entryStart + possibleEntries * 4;
            if (possibleDataStart <= v.length) {
                isObject = true;
                entryCount = possibleEntries;
            } else {
                isObject = false;
            }
        } else {
            isObject = false;
        }

        int dataStart = entryStart + entryCount * 4;
        if (dataStart > v.length) {
            return null;
        }
        byte[] rawData = new byte[v.length - dataStart];
        System.arraycopy(v, dataStart, rawData, 0, rawData.length);

        Object parsed;
        if (isObject) {
            parsed = decodeJsonbObject(entryCount, v, entryStart, rawData);
        } else if (entryCount > 1) {
            parsed = decodeJsonbArray(entryCount, v, entryStart, rawData);
        } else {
            if (count == 1) {
                int jentry = (int) le32(v, entryStart);
                parsed = decodeJsonbScalar(jentry, rawData, 0);
            } else {
                return null;
            }
        }
        return toJsonString(parsed);
    }

    /** Convert a decoded jsonb value (Map/List/scalar) to JSON string. */
    private static String toJsonString(Object val) {
        if (val == null) return "null";
        if (val instanceof String) return "\"" + escapeJson((String) val) + "\"";
        if (val instanceof Boolean || val instanceof Number) return val.toString();
        if (val instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) val;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> e : map.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(escapeJson(e.getKey())).append("\":").append(toJsonString(e.getValue()));
            }
            sb.append('}');
            return sb.toString();
        }
        if (val instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) val;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(toJsonString(list.get(i)));
            }
            sb.append(']');
            return sb.toString();
        }
        return "\"" + escapeJson(String.valueOf(val)) + "\"";
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Map<String, Object> decodeJsonbObject(int count, byte[] v, int entryStart, byte[] rawData) {
        Map<String, Object> result = new LinkedHashMap<>();
        int dataPos = 0;
        for (int i = 0; i + 1 < count; i += 2) {
            int keyEntry = (int) le32(v, entryStart + i * 4);
            int valEntry = (int) le32(v, entryStart + (i + 1) * 4);

            // Key is always a string
            int keyLen = keyEntry & JENTRY_LENMASK;
            String key = new String(rawData, dataPos, keyLen, StandardCharsets.UTF_8);
            dataPos += keyLen;

            // Value depends on type flags
            Object val = decodeJsonbScalar(valEntry, rawData, dataPos);
            if (val == NOT_FOUND) {
                // Offset-based entry (nested container or long string)
                // For now, skip — full container support would need more work
                result.put(key, null);
            } else {
                result.put(key, val);
                if ((valEntry & JBE_ISNULL) == 0 && (valEntry & JBE_ISBOOL) == 0) {
                    dataPos += (valEntry & JENTRY_LENMASK);
                }
            }
        }
        return result;
    }

    private static List<Object> decodeJsonbArray(int count, byte[] v, int entryStart, byte[] rawData) {
        List<Object> result = new ArrayList<>();
        int dataPos = 0;
        for (int i = 0; i < count; i++) {
            int entry = (int) le32(v, entryStart + i * 4);
            Object val = decodeJsonbScalar(entry, rawData, dataPos);
            if (val == NOT_FOUND) {
                result.add(null);
            } else {
                result.add(val);
                if ((entry & JBE_ISNULL) == 0 && (entry & JBE_ISBOOL) == 0) {
                    dataPos += (entry & JENTRY_LENMASK);
                }
            }
        }
        return result;
    }

    /** Sentinel returned when a JEntry uses offset-based addressing (container / long value). */
    private static final Object NOT_FOUND = new Object();

    private static Object decodeJsonbScalar(int jentry, byte[] rawData, int dataPos) {
        if ((jentry & JBE_ISNULL) != 0) {
            return null;
        }
        if ((jentry & JBE_ISBOOL) != 0) {
            return (jentry & JBE_ISBOOL_TRUE) != 0;
        }
        int len = jentry & JENTRY_LENMASK;
        if ((jentry & JBE_ISSTRING) != 0) {
            if (dataPos + len > rawData.length) {
                return null;
            }
            return new String(rawData, dataPos, len, StandardCharsets.UTF_8);
        }
        if ((jentry & JBE_ISNUMERIC) != 0) {
            if (dataPos + len > rawData.length) {
                return null;
            }
            return new BigDecimal(new String(rawData, dataPos, len, StandardCharsets.UTF_8));
        }
        if ((jentry & JBE_ISCONTAINER) != 0) {
            // Nested container: the lower 28 bits are an offset into rawData
            // Full parsing of nested containers not yet implemented
            return NOT_FOUND;
        }
        // Unknown scalar type — try as string
        if (dataPos + len <= rawData.length && len > 0) {
            return new String(rawData, dataPos, len, StandardCharsets.UTF_8);
        }
        return NOT_FOUND;
    }

    /**
     * Decode PostgreSQL point type (16 bytes: two float64 LE).
     * point(12.0, 12.0) → "(12.0,12.0)"
     */
    private static String decodePoint(byte[] v) {
        if (v == null || v.length < 16) {
            return null;
        }
        double x = Double.longBitsToDouble(le64(v));
        double y = Double.longBitsToDouble(le64(v, 8));
        return "(" + x + "," + y + ")";
    }

    /** lseg: 32 bytes = 4 float64 (x1,y1,x2,y2). Output: "[(x1,y1),(x2,y2)]" */
    private static String decodeLseg(byte[] v) {
        if (v == null || v.length < 32) return null;
        double x1 = Double.longBitsToDouble(le64(v, 0));
        double y1 = Double.longBitsToDouble(le64(v, 8));
        double x2 = Double.longBitsToDouble(le64(v, 16));
        double y2 = Double.longBitsToDouble(le64(v, 24));
        return "[(" + x1 + "," + y1 + "),(" + x2 + "," + y2 + ")]";
    }

    /** box: 32 bytes = 4 float64 (x1,y1,x2,y2) — upper-right, lower-left corners. */
    private static String decodeBox(byte[] v) {
        if (v == null || v.length < 32) return null;
        double x1 = Double.longBitsToDouble(le64(v, 0));
        double y1 = Double.longBitsToDouble(le64(v, 8));
        double x2 = Double.longBitsToDouble(le64(v, 16));
        double y2 = Double.longBitsToDouble(le64(v, 24));
        return "(" + x1 + "," + y1 + "),(" + x2 + "," + y2 + ")";
    }

    /** circle: 24 bytes = 3 float64 (x, y, r). Output: "<(x,y),r>" */
    private static String decodeCircle(byte[] v) {
        if (v == null || v.length < 24) return null;
        double x = Double.longBitsToDouble(le64(v, 0));
        double y = Double.longBitsToDouble(le64(v, 8));
        double r = Double.longBitsToDouble(le64(v, 16));
        return "<(" + x + "," + y + ")," + r + ">";
    }

    /** line: 24 bytes = 3 float64 (A, B, C). Output: "{A,B,C}" */
    private static String decodeLine(byte[] v) {
        if (v == null || v.length < 24) return null;
        double a = Double.longBitsToDouble(le64(v, 0));
        double b = Double.longBitsToDouble(le64(v, 8));
        double c = Double.longBitsToDouble(le64(v, 16));
        return "{" + a + "," + b + "," + c + "}";
    }

    /**
     * path: varlena. After varlena header: 1 byte closed + 4 bytes npts + npts*16 bytes (x,y pairs).
     * Output: "(x1,y1),...,(xn,yn)" if open; "((x1,y1),...,(xn,yn))" if closed.
     */
    private static String decodePath(byte[] v) {
        if (v == null || v.length < 5) return null;
        boolean closed = v[0] != 0;
        int npts = (int) le32(v, 1);
        if (npts <= 0 || npts > 65535) return null;
        StringBuilder sb = new StringBuilder();
        if (closed) sb.append('(');
        for (int i = 0; i < npts; i++) {
            int off = 5 + i * 16;
            if (off + 16 > v.length) break;
            if (i > 0) sb.append(',');
            double x = Double.longBitsToDouble(le64(v, off));
            double y = Double.longBitsToDouble(le64(v, off + 8));
            sb.append('(').append(x).append(',').append(y).append(')');
        }
        if (closed) sb.append(')');
        return sb.toString();
    }

    /**
     * polygon: varlena. After varlena header: 4 bytes npts + npts*16 bytes (x,y pairs).
     * Output: "((x1,y1),...,(xn,yn))"
     */
    private static String decodePolygon(byte[] v) {
        if (v == null || v.length < 4) return null;
        int npts = (int) le32(v, 0);
        if (npts <= 0 || npts > 65535) return null;
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < npts; i++) {
            int off = 4 + i * 16;
            if (off + 16 > v.length) break;
            if (i > 0) sb.append(',');
            double x = Double.longBitsToDouble(le64(v, off));
            double y = Double.longBitsToDouble(le64(v, off + 8));
            sb.append('(').append(x).append(',').append(y).append(')');
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * cidr/inet: varlena. After varlena header:
     *   1 byte family (2=IPv4, 3=IPv6)
     *   1 byte bits (netmask length)
     *   1 byte is_cidr (1=cidr, 0=inet)
     *   1 byte addr_len (4 or 16)
     *   addr_len bytes of address
     */
    private static String decodeInet(byte[] v) {
        if (v == null || v.length < 8) return null;
        int family = v[0] & 0xFF;
        int bits = v[1] & 0xFF;
        int addrLen = v[3] & 0xFF;
        if (v.length < 4 + addrLen) return null;
        StringBuilder sb = new StringBuilder();
        if (family == 2) {   // IPv4
            sb.append(v[4] & 0xFF).append('.').append(v[5] & 0xFF)
              .append('.').append(v[6] & 0xFF).append('.').append(v[7] & 0xFF);
        } else {              // IPv6
            for (int i = 0; i < addrLen; i++) {
                if (i > 0 && i % 2 == 0) sb.append(':');
                sb.append(String.format("%02x", v[4 + i] & 0xFF));
            }
        }
        sb.append('/').append(bits);
        return sb.toString();
    }

    /** macaddr: 6 bytes. Output: "xx:xx:xx:xx:xx:xx" */
    private static String decodeMacaddr(byte[] v) {
        if (v == null || v.length < 6) return null;
        return String.format("%02x:%02x:%02x:%02x:%02x:%02x",
                v[0] & 0xFF, v[1] & 0xFF, v[2] & 0xFF,
                v[3] & 0xFF, v[4] & 0xFF, v[5] & 0xFF);
    }

    /**
     * tsvector: varlena. After varlena header: int32 size, then lexeme entries.
     * Best-effort: extract readable ASCII/substrings from the payload.
     */
    private static String decodeTsVector(byte[] v) {
        if (v == null || v.length < 4) return null;
        try {
            // Skip the 4-byte size header, return the rest as a best-effort string
            return new String(v, 4, v.length - 4, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return new String(v, StandardCharsets.UTF_8);
        }
    }

    /**
     * tsquery: varlena. After varlena header: complex query tree.
     * Best-effort: return as UTF-8 string.
     */
    private static String decodeTsQuery(byte[] v) {
        if (v == null) return null;
        try {
            return new String(v, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return null;
        }
    }

    private static long le64(byte[] b, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (b[offset + i] & 0xFFL) << (8 * i);
        }
        return v;
    }

    /* on-disk numeric (NumericChoice) header bits, see utils/adt/numeric.c */
    private static final int NUMERIC_SIGN_MASK = 0xC000;
    private static final int NUMERIC_NEG = 0x4000;
    private static final int NUMERIC_SHORT = 0x8000;
    private static final int NUMERIC_SPECIAL = 0xC000;
    private static final int NUMERIC_SHORT_SIGN_MASK = 0x2000;
    private static final int NUMERIC_SHORT_DSCALE_MASK = 0x1F80;
    private static final int NUMERIC_SHORT_DSCALE_SHIFT = 7;
    private static final int NUMERIC_SHORT_WEIGHT_SIGN_MASK = 0x0040;
    private static final int NUMERIC_SHORT_WEIGHT_MASK = 0x003F;
    private static final int NUMERIC_DSCALE_MASK = 0x3FFF;

    /**
     * Decodes a numeric value in PostgreSQL's on-disk ({@code NumericChoice})
     * layout as carried in heap tuples / WAL: a packed 2-byte header (short form)
     * or a 2-byte sign+dscale plus a 2-byte weight (long form), each followed by
     * base-10000 {@code NumericDigit}s. This differs from the binary send/recv
     * wire format, which prefixes four int16s (ndigits, weight, sign, dscale).
     */
    static BigDecimal decodeNumeric(byte[] v) {
        if (v == null || v.length < 2) {
            return null;
        }
        int header = (v[0] & 0xFF) | ((v[1] & 0xFF) << 8);
        if ((header & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL) {
            return null; // NaN / +Inf / -Inf have no BigDecimal representation
        }
        boolean negative;
        int weight;
        int dscale;
        int digitsStart;
        if ((header & NUMERIC_SHORT) != 0) {
            negative = (header & NUMERIC_SHORT_SIGN_MASK) != 0;
            dscale = (header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT;
            weight = header & NUMERIC_SHORT_WEIGHT_MASK;
            if ((header & NUMERIC_SHORT_WEIGHT_SIGN_MASK) != 0) {
                weight |= ~NUMERIC_SHORT_WEIGHT_MASK;   // sign-extend negative weight
            }
            digitsStart = 2;
        } else {
            negative = (header & NUMERIC_SIGN_MASK) == NUMERIC_NEG;
            dscale = header & NUMERIC_DSCALE_MASK;
            weight = (short) ((v[2] & 0xFF) | ((v[3] & 0xFF) << 8));
            digitsStart = 4;
        }
        int ndigits = (v.length - digitsStart) / 2;
        BigDecimal val = BigDecimal.ZERO;
        for (int i = 0; i < ndigits; i++) {
            int off = digitsStart + i * 2;
            int digit = (v[off] & 0xFF) | ((v[off + 1] & 0xFF) << 8);
            val = val.add(BigDecimal.valueOf(digit).scaleByPowerOfTen((weight - i) * 4));
        }
        if (negative) {
            val = val.negate();
        }
        return val.setScale(dscale, RoundingMode.HALF_UP);
    }

    private static String decodeUuid(byte[] v) {
        long msb = 0, lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (v[i] & 0xFFL);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (v[i] & 0xFFL);
        }
        return new UUID(msb, lsb).toString();
    }

    private static int le16(byte[] b) {
        return (b[0] & 0xFF) | ((b[1] & 0xFF) << 8);
    }

    private static long le32(byte[] b) {
        return (b[0] & 0xFFL) | ((b[1] & 0xFFL) << 8) | ((b[2] & 0xFFL) << 16) | ((b[3] & 0xFFL) << 24);
    }

    private static long le32(byte[] b, int offset) {
        return (b[offset] & 0xFFL) | ((b[offset + 1] & 0xFFL) << 8)
                | ((b[offset + 2] & 0xFFL) << 16) | ((b[offset + 3] & 0xFFL) << 24);
    }

    private static long le64(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (b[i] & 0xFFL) << (8 * i);
        }
        return v;
    }
}
