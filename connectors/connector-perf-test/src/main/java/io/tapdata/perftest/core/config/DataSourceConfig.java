package io.tapdata.perftest.core.config;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.perftest.core.client.DataSourceClient;

/**
 * 数据源配置抽象基类。
 * 子类持有对应 connector 所需的连接参数，并通过 createClient() 工厂方法创建 DataSourceClient。
 *
 * 遵循依赖倒置原则：测试引擎只依赖此抽象类，不依赖任何具体实现。
 */
public abstract class DataSourceConfig {

    private String dataSourceType;   // "paimon" | "mysql" | "kafka" ...
    private String database = "default";
    private TapTable tapTable;       // 测试表定义（Schema + 主键）

    /** 工厂方法：创建对应的 DataSourceClient 实例。 */
    public abstract DataSourceClient createClient();

    public String getDataSourceType()            { return dataSourceType; }
    public void setDataSourceType(String t)      { this.dataSourceType = t; }
    public String getDatabase()                  { return database; }
    public void setDatabase(String d)            { this.database = d; }
    public TapTable getTapTable()                { return tapTable; }
    public void setTapTable(TapTable t)          { this.tapTable = t; }
}
