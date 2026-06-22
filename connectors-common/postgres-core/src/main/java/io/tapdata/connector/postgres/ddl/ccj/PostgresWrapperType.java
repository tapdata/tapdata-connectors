package io.tapdata.connector.postgres.ddl.ccj;

import io.tapdata.common.ddl.type.DDLType;
import io.tapdata.common.ddl.type.WrapperType;

public class PostgresWrapperType extends WrapperType {
    public PostgresWrapperType() {
        this.ddlTypes.add(new DDLType(DDLType.Type.ADD_COLUMN, "alter\\s+table\\s+.*\\s+add\\s+(column\\s+){0,1}((?!(index|constraint|fulltext|key|spatial|partition)).)+.+", false, "ADD [COLUMN] (col_name column_definition,...)", PostgresAddColumnDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.MODIFY_COLUMN, "alter\\s+table\\s+.*\\s+alter\\s+(column){0,1}.+", false, "MODIFY [COLUMN] col_name column_definition", PostgresAlterColumnAttrsDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.RENAME_COLUMN, "alter\\s+table\\s+.*\\s+rename\\s+(column\\s+){0,1}.+\\s+to\\s+.+", false, "RENAME COLUMN old_col_name TO new_col_name", PostgresAlterColumnNameDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.DROP_COLUMN, "alter\\s+table\\s+.*\\s+drop\\s+(column\\s+){0,1}((?!(check|constraint|index|key|primary\\skey|foreign\\skey)).)+.+", false, "DROP [COLUMN] col_name", PostgresDropColumnDDLWrapper.class));
    }
}
