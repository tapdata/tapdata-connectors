package io.tapdata.connector.postgres.cdc.physical;

import java.util.Collections;
import java.util.Map;

/**
 * Physical-storage metadata for a single column, mirroring the fields of
 * pg_attribute that drive heap-tuple deformation: attribute number, on-disk
 * type oid, length ({@code attlen}), alignment ({@code attalign}) and whether
 * the column has been dropped. Column order follows physical attnum order.
 *
 * @author Jarad
 */
public class ColumnInfo {

    public final String name;
    public final int attnum;
    public final long typeOid;
    /** attlen: >0 fixed, -1 varlena, -2 cstring. */
    public final int typLen;
    /** attalign as a byte boundary: 1 ('c'), 2 ('s'), 4 ('i'), 8 ('d'). */
    public final int typAlign;
    public final boolean dropped;
    /** For enum columns this is the type OID; for enum[] columns this is the element type OID. */
    public final long enumTypeOid;
    /** Maps pg_enum.oid values stored on disk to enum labels. */
    public final Map<Long, String> enumLabels;

    public ColumnInfo(String name, int attnum, long typeOid, int typLen, char typAlign, boolean dropped) {
        this(name, attnum, typeOid, typLen, typAlign, dropped, 0L, null);
    }

    public ColumnInfo(String name, int attnum, long typeOid, int typLen, char typAlign, boolean dropped,
                      long enumTypeOid, Map<Long, String> enumLabels) {
        this.name = name;
        this.attnum = attnum;
        this.typeOid = typeOid;
        this.typLen = typLen;
        this.typAlign = alignOf(typAlign);
        this.dropped = dropped;
        this.enumTypeOid = enumTypeOid;
        this.enumLabels = enumLabels == null ? Collections.emptyMap() : Collections.unmodifiableMap(enumLabels);
    }

    private static int alignOf(char a) {
        switch (a) {
            case 'd':
                return 8;
            case 'i':
                return 4;
            case 's':
                return 2;
            case 'c':
            default:
                return 1;
        }
    }
}
