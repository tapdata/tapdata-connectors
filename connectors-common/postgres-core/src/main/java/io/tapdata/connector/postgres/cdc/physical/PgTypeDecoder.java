package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapStringValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.*;
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

    // pg_type typlen for common element types (negative = varlena)
    private static final java.util.Map<Long, Integer> TYPE_LEN = new java.util.HashMap<>();
    static {
        TYPE_LEN.put(BOOL, 1);
        TYPE_LEN.put(INT2, 2);
        TYPE_LEN.put(INT4, 4);
        TYPE_LEN.put(INT8, 8);
        TYPE_LEN.put(OID, 4);
        TYPE_LEN.put(FLOAT4, 4);
        TYPE_LEN.put(FLOAT8, 8);
        TYPE_LEN.put(DATE, 4);
        TYPE_LEN.put(TIME, 8);
        TYPE_LEN.put(TIMESTAMP, 8);
        TYPE_LEN.put(TIMESTAMPTZ, 8);
        TYPE_LEN.put(TIMETZ, 12);
        TYPE_LEN.put(UUID_OID, 16);
        TYPE_LEN.put(MONEY, 8);
        TYPE_LEN.put(INTERVAL, 16);
    }

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
        // Try array type (int[], varchar[], text[], timestamp[], etc.)
        Object arrResult = decodeArray(v);
        if (arrResult != null) {
            return arrResult;
        }
        return new String(v, StandardCharsets.UTF_8);
    }

    /**
     * Decode a PostgreSQL array on-disk representation into a Java List.
     *
     * On-disk ArrayType layout (after varlena header stripped):
     *   int32  ndim               — number of dimensions
     *   int32  dataoffset         — 0 = no null bitmap, otherwise offset to element data
     *   int32  elemtype           — OID of the element type
     *   For each dimension: int32 dim, int32 lbound
     *   Elements:
     *     — fixed-length (typlen &gt; 0): raw bytes, typlen per element
     *     — variable-length (typlen &lt; 0): full varlena datum per element
     */
    private static Object decodeArray(byte[] v) {
        if (v == null || v.length < 12) return null;
        int ndim = (int) le32(v, 0);
        if (ndim < 1 || ndim > 6) return null;
        int dataOffset = (int) le32(v, 4);
        if (dataOffset < 0 || dataOffset > v.length) return null;

        // Try standard elemOid at offset 8; if invalid, try offset 9
        // (varlena header remnants in WAL images may shift array fields by 1–3 bytes,
        //  but ndim=1 and dataoffset=0 happen to mask the shift for small-OID types)
        int elemOff = 8;
        long elemOid = le32(v, elemOff);
        if (elemOid < 1 || elemOid > 10000) {
            elemOff = 9;
            if (v.length > elemOff + 3) {
                elemOid = le32(v, elemOff);
            }
            if (elemOid < 1 || elemOid > 10000) {
                return null;
            }
        }
        int headerLen = elemOff + 4; // ndim(4) + dataoffset(4) [+ pad] + elemOid(4)

        int pos = headerLen;
        int[] dims = new int[ndim];
        int total = 1;
        for (int d = 0; d < ndim; d++) {
            if (pos + 8 > v.length) return null;
            dims[d] = (int) le32(v, pos);
            if (dims[d] <= 0 || dims[d] > 1000000) return null;
            total *= dims[d];
            pos += 8; // skip lbound
        }

        // Skip null bitmap if present
        byte[] nullBitmap = null;
        if (dataOffset != 0) {
            int bitmapBytes = (total + 7) / 8;
            if (pos + bitmapBytes > v.length) return null;
            nullBitmap = new byte[bitmapBytes];
            System.arraycopy(v, pos, nullBitmap, 0, bitmapBytes);
            pos = dataOffset;
        }

        // Determine element format
        Integer typlen = TYPE_LEN.get(elemOid);
        boolean isFixed = typlen != null && typlen > 0;

        List<Object> values = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            if (nullBitmap != null && ((nullBitmap[i >> 3] & (1 << (i & 7))) == 0)) {
                values.add(null);
                continue;
            }

            byte[] elemBytes;
            if (isFixed) {
                // Fixed-length: typlen bytes per element, no length prefix
                if (pos + typlen > v.length) return null;
                elemBytes = new byte[typlen];
                System.arraycopy(v, pos, elemBytes, 0, typlen);
                pos += typlen;
            } else {
                VarlenaDatum datum = readArrayVarlena(v, pos);
                if (datum == null) return null;
                pos += datum.consumed;
                elemBytes = datum.value;
            }

            values.add(decode(elemOid, elemBytes));
        }

        return ndim == 1 ? values : nestArray(values, dims, 0, 0).value;
    }

    private static VarlenaDatum readArrayVarlena(byte[] data, int offset) {
        if (offset >= data.length) return null;
        int first = data[offset] & 0xFF;

        if ((first & 0x01) == 0x01) {
            if (first == 0x01) return null; // external TOAST pointer is not in the tuple image.
            int total = (first >> 1) & 0x7F;
            if (total < 1 || offset + total > data.length) return null;
            byte[] value = new byte[total - 1];
            System.arraycopy(data, offset + 1, value, 0, value.length);
            return new VarlenaDatum(value, total);
        }

        if (offset + 4 > data.length) return null;
        int header = (int) le32(data, offset);
        int total = (header >> 2) & 0x3FFFFFFF;
        if (total < 4 || offset + total > data.length) return null;
        byte[] value = new byte[total - 4];
        System.arraycopy(data, offset + 4, value, 0, value.length);
        return new VarlenaDatum(value, intAlign(total));
    }

    private static NestedArray nestArray(List<Object> values, int[] dims, int depth, int index) {
        int size = dims[depth];
        List<Object> out = new ArrayList<>(size);
        if (depth == dims.length - 1) {
            for (int i = 0; i < size; i++) {
                out.add(values.get(index + i));
            }
            return new NestedArray(out, index + size);
        }
        int next = index;
        for (int i = 0; i < size; i++) {
            NestedArray nested = nestArray(values, dims, depth + 1, next);
            out.add(nested.value);
            next = nested.nextIndex;
        }
        return new NestedArray(out, next);
    }

    private static int intAlign(int n) {
        return (n + 3) & ~3;
    }

    private static final class VarlenaDatum {
        final byte[] value;
        final int consumed;

        VarlenaDatum(byte[] value, int consumed) {
            this.value = value;
            this.consumed = consumed;
        }
    }

    private static final class NestedArray {
        final List<Object> value;
        final int nextIndex;

        NestedArray(List<Object> value, int nextIndex) {
            this.value = value;
            this.nextIndex = nextIndex;
        }
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
        return time.plusSeconds(offset.getTotalSeconds()).atDate(LocalDate.ofYearDay(1970, 1)).atZone(ZoneOffset.UTC);
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

    private static final int JB_CMASK = 0x0FFFFFFF;
    private static final int JB_FSCALAR = 0x10000000;
    private static final int JB_FOBJECT = 0x20000000;
    private static final int JB_FARRAY = 0x40000000;

    private static final int JENTRY_OFFLENMASK = 0x0FFFFFFF;
    private static final int JENTRY_TYPEMASK = 0x70000000;
    private static final int JENTRY_HAS_OFF = 0x80000000;
    private static final int JENTRY_ISSTRING = 0x00000000;
    private static final int JENTRY_ISNUMERIC = 0x10000000;
    private static final int JENTRY_ISBOOL_FALSE = 0x20000000;
    private static final int JENTRY_ISBOOL_TRUE = 0x30000000;
    private static final int JENTRY_ISNULL = 0x40000000;
    private static final int JENTRY_ISCONTAINER = 0x50000000;

    /** Sentinel indicating parse failure (vs. valid JSON null) */
    private static final Object PARSE_FAILED = new Object();

    /**
     * Decode PostgreSQL jsonb on-disk format. The varlena header has already
     * been stripped, so the input starts with JsonbContainer.header.
     */
    private static Object decodeJsonb(byte[] v) {
        if (v == null) {
            return null;
        }
        Object parsed = decodeJsonbValue(v, 0, v.length);
        // Return null for parse failures, otherwise convert to JSON string
        return (parsed == PARSE_FAILED) ? null : toJsonString(parsed);
    }

    private static Object decodeJsonbValue(byte[] v, int offset, int length) {
        if (v == null || length < 4 || offset < 0 || offset + length > v.length) {
            return PARSE_FAILED;
        }

        int header = (int) le32(v, offset);
        int flags = header & 0xF0000000;
        int count = header & JB_CMASK;
        boolean scalar = (flags & JB_FSCALAR) != 0;
        boolean object = (flags & JB_FOBJECT) != 0;
        boolean array = (flags & JB_FARRAY) != 0;

        // Validate container type flags: exactly one of object/array must be set (XOR),
        // and if scalar is set, it must be with array only
        if ((object && array) || (!object && !array) || (scalar && object)) {
            return PARSE_FAILED;
        }

        // Handle empty containers
        if (count == 0) {
            if (object) {
                return new LinkedHashMap<>();
            }
            if (array && !scalar) {
                return new ArrayList<>();
            }
            return PARSE_FAILED;
        }

        // Scalar must have exactly 1 element
        if (scalar && count != 1) {
            return PARSE_FAILED;
        }

        // Calculate entry count: objects have key+value pairs
        int entryCount = object ? count * 2 : count;
        if (entryCount <= 0 || entryCount > (length - 4) / 4) {
            return PARSE_FAILED;
        }

        // Parse all JEntry headers and compute data offsets/lengths
        int entryStart = offset + 4;
        int dataStart = entryStart + entryCount * 4;
        int end = offset + length;
        int[] entries = new int[entryCount];
        int[] dataOffsets = new int[entryCount];
        int[] dataLengths = new int[entryCount];
        int dataPos = 0;
        for (int i = 0; i < entryCount; i++) {
            int entry = (int) le32(v, entryStart + i * 4);
            int offLen = entry & JENTRY_OFFLENMASK;
            int len;
            if ((entry & JENTRY_HAS_OFF) != 0) {
                len = offLen - dataPos;
            } else {
                len = offLen;
            }
            if (len < 0 || dataStart + dataPos + len > end) {
                return PARSE_FAILED;
            }
            entries[i] = entry;
            dataOffsets[i] = dataStart + dataPos;
            dataLengths[i] = len;
            dataPos += len;
        }

        // Decode based on container type
        if (scalar) {
            return decodeJsonbScalar(entries[0], v, dataOffsets[0], dataLengths[0]);
        }
        if (array) {
            List<Object> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Object elem = decodeJsonbScalar(entries[i], v, dataOffsets[i], dataLengths[i]);
                result.add(elem);
            }
            return result;
        }

        // Object: keys in first half, values in second half
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String key = decodeJsonbKey(entries[i], v, dataOffsets[i], dataLengths[i]);
            if (key == null) {
                return PARSE_FAILED;
            }
            Object value = decodeJsonbScalar(entries[count + i], v, dataOffsets[count + i], dataLengths[count + i]);
            result.put(key, value);
        }
        return result;
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
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    private static String decodeJsonbKey(int jentry, byte[] data, int dataOffset, int dataLength) {
        // Keys must be strings
        if ((jentry & JENTRY_TYPEMASK) != JENTRY_ISSTRING) {
            return null; // Return null to indicate parse failure (checked by caller)
        }
        return new String(data, dataOffset, dataLength, StandardCharsets.UTF_8);
    }

    private static Object decodeJsonbScalar(int jentry, byte[] data, int dataOffset, int dataLength) {
        switch (jentry & JENTRY_TYPEMASK) {
            case JENTRY_ISSTRING:
                return new String(data, dataOffset, dataLength, StandardCharsets.UTF_8);
            case JENTRY_ISNUMERIC:
                return decodeJsonbNumeric(data, dataOffset, dataLength);
            case JENTRY_ISBOOL_FALSE:
                return false;
            case JENTRY_ISBOOL_TRUE:
                return true;
            case JENTRY_ISNULL:
                return null;
            case JENTRY_ISCONTAINER:
                return decodeJsonbContainer(data, dataOffset, dataLength);
            default:
                // Unknown type, treat as null
                return null;
        }
    }

    /**
     * Decode nested JSONB container (for JENTRY_ISCONTAINER type).
     * May have 0-3 bytes of padding before the actual container header.
     */
    private static Object decodeJsonbContainer(byte[] data, int dataOffset, int dataLength) {
        if (dataLength < 4) {
            return PARSE_FAILED;
        }

        // Try padding 0-3 bytes
        int maxPad = Math.min(3, dataLength - 4);
        for (int pad = 0; pad <= maxPad; pad++) {
            // Stop if we hit non-zero padding
            if (pad > 0 && data[dataOffset + pad - 1] != 0) {
                break;
            }

            Object value = decodeJsonbValue(data, dataOffset + pad, dataLength - pad);
            if (value != PARSE_FAILED) {
                return value;
            }

            // Special case: scalar null container (valid JSON null)
            // Header: JB_FSCALAR | JB_FARRAY, count=1, JEntry type=JENTRY_ISNULL
            int remainingLength = dataLength - pad;
            if (remainingLength >= 8) {
                int header = (int) le32(data, dataOffset + pad);
                int jentry = (int) le32(data, dataOffset + pad + 4);
                if ((header & 0xF0000000) == (JB_FSCALAR | JB_FARRAY)
                        && (header & JB_CMASK) == 1
                        && (jentry & JENTRY_TYPEMASK) == JENTRY_ISNULL) {
                    return null; // This is a valid JSON null
                }
            }
        }

        return PARSE_FAILED;
    }

    /**
     * Decode JSONB numeric value, handling alignment padding and varlena wrapper.
     * JSONB numeric data may have 0-3 bytes of padding for alignment, followed by:
     * - 4-byte varlena header (for longer numerics), OR
     * - 1-byte varlena header (for short numerics)
     * Then the actual numeric data.
     */
    private static BigDecimal decodeJsonbNumeric(byte[] data, int dataOffset, int dataLength) {
        if (dataLength < 2) {
            return null;
        }

        // Try padding 0-3 bytes (for alignment)
        int maxPad = Math.min(3, dataLength - 2);
        for (int pad = 0; pad <= maxPad; pad++) {
            // Stop if we hit non-zero padding
            if (pad > 0 && data[dataOffset + pad - 1] != 0) {
                break;
            }

            int offset = dataOffset + pad;
            int length = dataLength - pad;

            // Try 4-byte varlena header format
            if (length >= 4) {
                long header = le32(data, offset);
                int tag = (int) (header & 0x03);
                int total = (int) ((header >> 2) & 0x3FFFFFFF);
                if (tag == 0 && total >= 6 && total <= length) {
                    byte[] numeric = new byte[total - 4];
                    System.arraycopy(data, offset + 4, numeric, 0, numeric.length);
                    BigDecimal result = decodeNumeric(numeric);
                    if (result != null) {
                        return result;
                    }
                }
            }

            // Try 1-byte varlena header format
            if (length >= 2 && (data[offset] & 0x01) == 0x01) {
                int total = (data[offset] >> 1) & 0x7F;
                if (total >= 3 && total <= length) {
                    byte[] numeric = new byte[total - 1];
                    System.arraycopy(data, offset + 1, numeric, 0, numeric.length);
                    BigDecimal result = decodeNumeric(numeric);
                    if (result != null) {
                        return result;
                    }
                }
            }
        }

        return null;
    }

    private static boolean allZero(byte[] data, int offset, int length) {
        for (int i = 0; i < length; i++) {
            if (data[offset + i] != 0) {
                return false;
            }
        }
        return true;
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
     * path: varlena. PG on-disk PATH struct (after varlena header stripped):
     *   int32 npts;     // number of points
     *   int32 closed;   // 0=open, 1=closed
     *   int32 dummy;    // alignment padding to double-align points
     *   Point p[npts];  // each Point = 2 float64 (16 bytes)
     * Output: "(x1,y1),...,(xn,yn)" if open; "((x1,y1),...,(xn,yn))" if closed.
     */
    private static String decodePath(byte[] v) {
        if (v == null || v.length < 8) return null;
        int npts = (int) le32(v, 0);
        // PG stores closed as a 1-byte field at offset 4, then 3 pad bytes,
        // followed by an optional 4-byte dummy for double alignment.
        // Reading int32 at offset 4 may get garbage in padding/dummy bytes,
        // so read only byte 4 for the closed flag.
        boolean closed = v[4] != 0;
        if (npts <= 0 || npts > 65535) return null;
        int pointsStart = 12; // npts(4) + closed(1) + pad(3) + dummy(4) = 12
        if (pointsStart + npts * 16L > v.length) {
            // PG >= 15 may omit dummy; try without it
            pointsStart = 8;
            if (pointsStart + npts * 16L > v.length) return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(closed ? '(' : '[');
        for (int i = 0; i < npts; i++) {
            int off = pointsStart + i * 16;
            if (off + 16 > v.length) break;
            if (i > 0) sb.append(',');
            double x = Double.longBitsToDouble(le64(v, off));
            double y = Double.longBitsToDouble(le64(v, off + 8));
            sb.append('(').append(x).append(',').append(y).append(')');
        }
        sb.append(closed ? ')' : ']');
        return sb.toString();
    }

    /**
     * polygon: varlena. PG on-disk POLYGON struct (after varlena header stripped):
     *   int32 npts;          // number of points
     *   BOX boundbox;        // bounding box (32 bytes: 4 float64, high+low corners)
     *   Point p[npts];       // each Point = 2 float64 (16 bytes)
     * Output: "((x1,y1),...,(xn,yn))"
     */
    private static String decodePolygon(byte[] v) {
        if (v == null || v.length < 36) return null;
        int npts = (int) le32(v, 0);
        if (npts <= 0 || npts > 65535) return null;
        int pointsStart = 36; // npts(4) + bounding box(32)
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < npts; i++) {
            int off = pointsStart + i * 16;
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
     * cidr/inet: varlena. PG on-disk inet struct (after varlena header stripped):
     *   uint8  family;    // PGSQL_AF_INET=2 (IPv4), PGSQL_AF_INET6=3 (IPv6)
     *   uint8  bits;      // netmask length (e.g. 24 for /24)
     *   uint8  is_cidr;   // 1 = cidr type, 0 = inet type
     *   uint8  ipaddr_len;// address length in bytes (4 for IPv4, 16 for IPv6)
     *   uint8  ipaddr[ipaddr_len];  // address in network byte order
     */
    /**
     * cidr/inet: varlena. PG on-disk inet struct (after varlena header stripped):
     *   uint8  family;     // PGSQL_AF_INET=2 (IPv4), PGSQL_AF_INET6=3 (IPv6)
     *   uint8  bits;       // netmask length (e.g. 24 for /24)
     *   uint8  ipaddr[];   // address bytes in network order (4 for IPv4, 16 for IPv6)
     *   uint8  is_cidr;    // 1 = cidr type, 0 = inet type (last byte)
     * Total: 3 + ipaddr_len bytes. For IPv4: 7 bytes, IPv6: 19 bytes.
     * There is NO explicit ipaddr_len field — it's inferred from total varlena size.
     */
    /**
     * cidr/inet on-disk format (after varlena header stripped):
     *   family(1) + bits(1) + ip_addr(v.length - 2)
     * No is_cidr byte — that exists only in the wire-protocol binary format.
     * IPv4: 6 bytes total. IPv6: 18 bytes total.
     */
    private static String decodeInet(byte[] v) {
        if (v == null || v.length < 6) return null;
        int family = v[0] & 0xFF;
        int bits = v[1] & 0xFF;
        int addrLen = v.length - 2;          // family + bits = 2 bytes
        if (addrLen != 4 && addrLen != 16) return null;
        StringBuilder sb = new StringBuilder();
        if (family == 2) {   // IPv4
            sb.append(v[2] & 0xFF).append('.').append(v[3] & 0xFF)
              .append('.').append(v[4] & 0xFF).append('.').append(v[5] & 0xFF);
        } else {             // IPv6
            for (int i = 0; i < addrLen; i++) {
                if (i > 0 && i % 2 == 0) sb.append(':');
                sb.append(String.format("%02x", v[2 + i] & 0xFF));
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
     * tsvector: varlena. On-disk layout (after varlena header stripped):
     *   int32 size;              // number of lexemes
     *   WordEntry entries[size]; // each: haspos(1b) + len(11b) + pos(20b) = 32 bits
     *   char data[];             // null-terminated lexeme strings
     *
     * Fallback heuristic when WordEntry parsing fails: extract all printable
     * ASCII runs from the payload separated by spaces.
     */
    private static String decodeTsVector(byte[] v) {
        if (v == null || v.length < 4) return null;
        int size = (int) le32(v, 0);
        if (size <= 0 || size > 1000) return extractPrintableRuns(v, 0);
        int entryBytes = size * 4;
        if (4 + entryBytes > v.length) return extractPrintableRuns(v, 0);

        // Try WordEntry-based extraction: compute string section start
        int strStart = 4 + entryBytes;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            int we = (int) le32(v, 4 + i * 4);
            int haspos = we & 1;
            int len = (we >>> 1) & 0x7FF;
            int pos = (we >>> 12) & 0xFFFFF;
            if (len > 0 && strStart + pos + len <= v.length) {
                if (sb.length() > 0) sb.append(' ');
                sb.append(new String(v, strStart + pos, len, StandardCharsets.UTF_8));
            }
            // If haspos=1, skip position data after lexemes (handled by next entry's pos)
        }
        if (sb.length() > 0) return sb.toString();

        // Fallback: extract printable runs from the data section
        return extractPrintableRuns(v, strStart);
    }

    /**
     * tsquery: varlena. Complex query tree on disk.
     *   int32 size;              // number of query items
     *   QueryItem items[size];   // each packed QueryItem (12 bytes of bitfields)
     *   char data[];             // operand strings with null terminators
     *
     * QueryItem bitfield layout is complex (GCC-dependent packing). Rather than
     * attempting a fragile decode, skip the binary item array and extract
     * operand strings from the data section via printable-run scanning.
     */
    private static String decodeTsQuery(byte[] v) {
        if (v == null || v.length < 4) return null;
        int size = (int) le32(v, 0);
        if (size <= 0 || size > 1000) return extractPrintableRuns(v, 0);

        // Skip binary header: 4-byte size + size * 12-byte QueryItem array
        int strStart = 4 + size * 12;
        if (strStart < v.length) {
            String words = extractPrintableRuns(v, strStart);
            if (words != null) return words.replace(" ", " & ");
        }
        return extractPrintableRuns(v, 0);
    }

    /**
     * Extract printable ASCII sequences from binary data, joined by spaces.
     */
    private static String extractPrintableRuns(byte[] v, int start) {
        StringBuilder sb = new StringBuilder();
        StringBuilder run = new StringBuilder();
        for (int i = start; i < v.length; i++) {
            int b = v[i] & 0xFF;
            if (b >= 0x20 && b < 0x7F) {
                run.append((char) b);
            } else {
                if (run.length() > 0) {
                    if (sb.length() > 0) sb.append(' ');
                    sb.append(run);
                    run.setLength(0);
                }
            }
        }
        if (run.length() > 0) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(run);
        }
        return sb.length() > 0 ? sb.toString() : null;
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
