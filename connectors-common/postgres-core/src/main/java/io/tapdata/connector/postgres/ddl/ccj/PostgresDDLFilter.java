package io.tapdata.connector.postgres.ddl.ccj;

import io.tapdata.common.ddl.DDLFilter;
import io.tapdata.common.ddl.type.DDLType;

import java.util.regex.Pattern;

public class PostgresDDLFilter extends DDLFilter {

    @Override
    public String filterDDL(DDLType type, String ddl) {
        switch (type.getType()) {
            case RENAME_COLUMN:
                String lowerDdl = ddl.toLowerCase();
                if (!lowerDdl.matches(".*\\s+rename\\s+column\\s+.*\\s+to\\s+.*")) {
                    return ddl.replace("rename", "rename column");
                }
        }
        return super.filterDDL(type, ddl);
    }
}
