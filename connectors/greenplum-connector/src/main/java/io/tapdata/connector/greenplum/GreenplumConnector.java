package io.tapdata.connector.greenplum;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@TapConnectorClass("spec_greenplum.json")
public class GreenplumConnector extends PostgresConnector {

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //test
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        // target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        // source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        // query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);
        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> postgresJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                if (tapValue.getOriginType().endsWith(" array")) {
                    if (tapValue.getOriginValue() instanceof PgArray) {
                        return tapValue.getOriginValue();
                    } else {
                        return tapValue.getValue();
                    }
                }
                return toJson(tapValue.getValue());
            }
            return "null";
        });

        codecRegistry.registerToTapValue(PgArray.class, (value, tapType) -> {
            PgArray pgArray = (PgArray) value;
            try (
                    ResultSet resultSet = pgArray.getResultSet()
            ) {
                return new TapArrayValue(DbKit.getDataArrayByColumnName(resultSet, "VALUE"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PgSQLXML.class, (value, tapType) -> {
            try {
                return new TapStringValue(((PgSQLXML) value).getString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PGbox.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGline.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpath.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGobject.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(UUID.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> {
            if (postgresConfig.getOldVersionTimezone()) {
                return tapTimeValue.getValue().toTime();
            } else if (EmptyKit.isNotNull(tapTimeValue.getValue().getTimeZone())) {
                return tapTimeValue.getValue().toInstant().atZone(ZoneId.systemDefault()).toLocalTime();
            } else {
                return tapTimeValue.getValue().toInstant().atZone(postgresConfig.getZoneId()).toLocalTime();
            }
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (postgresConfig.getOldVersionTimezone() || EmptyKit.isNotNull(tapDateTimeValue.getValue().getTimeZone())) {
                return tapDateTimeValue.getValue().toTimestamp();
            } else {
                return tapDateTimeValue.getValue().toInstant().atZone(postgresConfig.getZoneId()).toLocalDateTime();
            }
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, "character(4)", TapValue::getOriginValue);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
    }

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectorContext.getConnectionConfig());
        postgresConfig.load(connectorContext.getNodeConfig());
        postgresJdbcContext = new GreenplumJdbcContext(postgresConfig);
        commonDbConfig = postgresConfig;
        jdbcContext = postgresJdbcContext;
        commonSqlMaker = new CommonSqlMaker();
        postgresVersion = postgresJdbcContext.queryVersion();
        postgresJdbcContext.withPostgresVersion(postgresVersion);
        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        tapLogger = connectorContext.getLog();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        exceptionCollector = new PostgresExceptionCollector();
    }

    protected void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new GreenplumRecordWriter(postgresJdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .setTapLogger(tapLogger)
                .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws SQLException {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                GreenplumTest greenplumTest = (GreenplumTest) new GreenplumTest(postgresConfig, consumer, connectionOptions).withPostgresVersion()
        ) {
            greenplumTest.testOneByOne();
            return connectionOptions;
        }
    }

    public List<TapTable> discoverPartitionInfo(List<TapTable> tapTableList) {
        return tapTableList;
    }

}
