package io.tapdata.connector.gauss.core;

import com.huawei.opengauss.jdbc.core.BaseConnection;
import com.huawei.opengauss.jdbc.jdbc.PgSQLXML;
import com.huawei.opengauss.jdbc.util.PGobject;
import io.tapdata.connector.postgres.dml.OldPostgresWriteRecorder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

public class GaussDBWriteRecorder extends OldPostgresWriteRecorder {

    public GaussDBWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    protected Object filterValue(Object value, String dataType) throws SQLException {
        if (EmptyKit.isNull(value)) {
            return null;
        }
        if ("uuid".equalsIgnoreCase(dataType)) {
            return value instanceof UUID ? value : UUID.fromString(String.valueOf(value));
        }
        if (dataType.startsWith("bit")) {
            PGobject pGobject = new PGobject();
            pGobject.setType("bit");
            if (value instanceof Boolean) {
                pGobject.setValue((Boolean) value ? "1" : "0");
            } else {
                pGobject.setValue(new String((byte[]) value));
            }
            return pGobject;
        }
        if (value instanceof List) {
            return connection.createArrayOf(dataType, ((List) value).toArray());
        }
        switch (dataType) {
            case "interval":
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
            case "money":
            case "cidr":
            case "inet":
            case "macaddr":
            case "json":
            case "geometry":
            case "jsonb":
            case "xmltype":
            case "hll":
            case "hash16":
            case "hash32":
            case "aclitem":
                PGobject pGobject = new PGobject();
                pGobject.setType(dataType);
                pGobject.setValue(String.valueOf(value));
                return pGobject;
            case "xml":
                return new PgSQLXML(connection.unwrap(BaseConnection.class), String.valueOf(value));
        }
        if (dataType.endsWith("with time zone") && value instanceof LocalDateTime) {
            Timestamp timestamp = Timestamp.valueOf(((LocalDateTime) value));
            timestamp.setTime(timestamp.getTime() + TimeZone.getDefault().getRawOffset());
            return timestamp;
        }
        if (value instanceof String) {
            return ((String) value).replace("\u0000", "");
        }
        if (value instanceof Boolean && dataType.contains("int")) {
            return Boolean.TRUE.equals(value) ? 1 : 0;
        }
        return value;
    }
}
