package io.tapdata.connector.snowflake;

import io.tapdata.common.CommonSqlMaker;

/**
 * Snowflake SQL Maker
 *
 * @author Jarad
 * @date 2026/03/24
 */
public class SnowflakeSqlMaker extends CommonSqlMaker {
    
    public SnowflakeSqlMaker() {
        // Snowflake uses double quotes for identifiers
        super('"');
    }
}

