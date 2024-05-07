package io.tapdata.zoho.service.connection;

import cn.hutool.json.JSONNull;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface ConnectionMode {
    String STRING_NORMAL = "StringNormal";
    String STRING_MINOR = "StringMinor";
    String DATE_TIME = "DateTime";
    String LONG = "Long";
    String EMAIL = "Email";
    String PHONE = "Phone";
    String TEXTAREA = "Textarea";
    String BOOLEAN = "Boolean";
    String INTEGER = "Integer";
    String NAME = "Name";
    String TYPE = "type";
    String NULL = "NULL";
    String ID = "id";
    String JAVA_ARRAY = "JAVA_Array";
    String MAP = "Map";
    String DATE = "Date";
    String URL = "URL";

    String DEPARTMENT = "department";
    String CONTACT = "contact";
    String ASSIGNEE = "assignee";

    Log getLog();

    TapConnectionContext getConnectionContext();

    default List<TapTable> discoverSchema(List<String> tables, int tableSize) {
        List<Schema> schemas = Schemas.allSupportSchemas();
        List<TapTable> tapTables = new ArrayList<>();
        if (null != schemas && !schemas.isEmpty()) {
            schemas.forEach(schema -> tapTables.addAll(schema.csv(tables, tableSize, getConnectionContext())));
        }
        return tapTables;
    }

    List<TapTable> discoverSchemaV1(List<String> tables, int tableSize);

    Map<String, Object> attributeAssignment(Map<String, Object> obj, String tableName, ZoHoBase openApi);

    Map<String, Object> attributeAssignmentSelf(Map<String, Object> obj, String tableName);

    ConnectionMode config(TapConnectionContext connectionContext);

    default void removeJsonNull(Map<String, Object> map) {
        if (null == map || map.isEmpty()) return;
        Iterator<Map.Entry<String, Object>> iteratorMap = map.entrySet().iterator();
        while (iteratorMap.hasNext()) {
            Map.Entry<String, Object> entry = iteratorMap.next();
            Object value = entry.getValue();
            if (value instanceof JSONNull) {
                iteratorMap.remove();
            } else if (value instanceof Map) {
                removeJsonNull((Map<String, Object>) value);
            } else if (value instanceof Collection) {
                try {
                    Collection<Map<String, Object>> list = (Collection<Map<String, Object>>) value;
                    list.forEach(this::removeJsonNull);
                } catch (Exception e) {
                    //doNothing
                }
            }
        }
    }

    static ConnectionMode getInstanceByName(TapConnectionContext connectionContext, String name) {
        if (Checker.isEmpty(name)) {
            throw new CoreException("Instance name must not be empty");
        }
        String className = String.format("io.tapdata.zoho.service.connection.impl.%s", name);
        try {
            Class<ConnectionMode> clz = (Class<ConnectionMode>) Class.forName(className);
            return clz.newInstance().config(connectionContext);
        } catch (ClassNotFoundException e) {
            throw new CoreException("Class not fund, need: {}", className);
        } catch (InstantiationException e1) {
            throw new CoreException("Failed to Instantiation, message: {}", e1.getMessage());
        } catch (IllegalAccessException e2) {
            throw new CoreException("Can not access class: {}", className);
        }
    }
}
