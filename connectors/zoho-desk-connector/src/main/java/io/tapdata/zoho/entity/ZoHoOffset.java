package io.tapdata.zoho.entity;

import io.tapdata.zoho.service.zoho.schema.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * ZoHo offset.
 *
 * @author <a href="https://github.com/11000100111010101100111">GavinXiao</a>
 * @version v1.0 2022/9/26 14:06 Create
 */
public class ZoHoOffset {
    protected static final String TABLE_UPDATE_TIME_MAP = "tableUpdateTimeMap";
    private Map<String, Long> tableUpdateTimeMap;
    public ZoHoOffset() {
        tableUpdateTimeMap = new HashMap<>();
    }

    public static ZoHoOffset create(Map<String, Long> tableUpdateTimeMap){
        ZoHoOffset offset = new ZoHoOffset();
        offset.setTableUpdateTimeMap(Optional.ofNullable(tableUpdateTimeMap).orElse(new HashMap<>()));
        return offset;
    }

    protected static Map<String, Long> map(Map<String, Long> tableUpdateTimeMap) {
        if (null == tableUpdateTimeMap) {
            tableUpdateTimeMap = new HashMap<>();
        }
        return new HashMap<>(tableUpdateTimeMap);
    }

    public static Map<String, Long> mapFromSchema(List<Schema> schemas, long date) {
        Map<String,Long> offset = new HashMap<>();
        if (null != schemas) {
            schemas.stream()
                    .filter(Objects::nonNull)
                    .forEach(schema -> offset.put(schema.schemaName(), date));
        }
        return offset;
    }

    public Map<String, Long> getTableUpdateTimeMap() {
        return tableUpdateTimeMap;
    }

    public void setTableUpdateTimeMap(Map<String, Long> tableUpdateTimeMap) {
        this.tableUpdateTimeMap = tableUpdateTimeMap;
    }

    public Object offset() {
        Map<String, Object> o = new HashMap<>();
        o.put(TABLE_UPDATE_TIME_MAP, map(tableUpdateTimeMap));
        return o;
    }

    public static ZoHoOffset from(Object o) {
        ZoHoOffset offset = new ZoHoOffset();
        if (o instanceof Map) {
            Object map = ((Map<String, Object>) o).get(TABLE_UPDATE_TIME_MAP);
            if (map instanceof Map) {
                offset.setTableUpdateTimeMap((Map<String, Long>) map);
            } else {
                offset.setTableUpdateTimeMap(new HashMap<>());
            }
        }
        return offset;
    }
}