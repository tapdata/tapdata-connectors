package io.debezium.connector.mysql.utils;

import com.mysql.cj.CharsetMapping;

public class CharsetMappingUtil {
    public static String getJavaEncodingForMysqlCharset(String mysqlCharset) {
        for (int index = 0; index < CharsetMapping.MAP_SIZE; index++) {
            if (mysqlCharset.equals(CharsetMapping.getStaticMysqlCharsetNameForCollationIndex(index))) {
                return CharsetMapping.getStaticJavaEncodingForCollationIndex(index);
            }
        }
        return null;
    }
}
