package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;

import java.io.Serializable;

public class TidbJdbcContext extends MysqlJdbcContextV2 implements Serializable {

    public TidbJdbcContext(CommonDbConfig config) {
        super(config);
    }


}
