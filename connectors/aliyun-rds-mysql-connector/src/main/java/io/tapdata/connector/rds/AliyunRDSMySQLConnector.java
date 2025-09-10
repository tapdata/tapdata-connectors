package io.tapdata.connector.rds;

import io.tapdata.connector.mysql.*;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.dml.sqlmaker.MysqlSqlMaker;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.ArrayList;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-15 11:42
 **/
@TapConnectorClass("aliyun-rds-mysql-spec.json")
public class AliyunRDSMySQLConnector extends MysqlConnector {

	@Override
	public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
		mysqlConfig = new AliyunRDSMySQLConfig().load(tapConnectionContext.getConnectionConfig());
		mysqlConfig.load(tapConnectionContext.getNodeConfig());
		contextMapForMasterSlave = MysqlUtil.buildContextMapForMasterSlave(mysqlConfig);
		MysqlUtil.buildMasterNode(mysqlConfig, contextMapForMasterSlave);
		MysqlJdbcContextV2 contextV2 = contextMapForMasterSlave.get(mysqlConfig.getHost() + mysqlConfig.getPort());
		if (null != contextV2) {
			mysqlJdbcContext = contextV2;
		} else {
			mysqlJdbcContext = new MysqlJdbcContextV2(mysqlConfig);
		}
		commonDbConfig = mysqlConfig;
		jdbcContext = mysqlJdbcContext;
		commonSqlMaker = new MysqlSqlMaker('`');
		if (Boolean.TRUE.equals(mysqlConfig.getCreateAutoInc())) {
			commonSqlMaker.createAutoInc(true);
		}
		if (Boolean.TRUE.equals(mysqlConfig.getApplyDefault())) {
			commonSqlMaker.applyDefault(true);
		}
		tapLogger = tapConnectionContext.getLog();
		exceptionCollector = new MysqlExceptionCollector();
		((MysqlExceptionCollector) exceptionCollector).setMysqlConfig(mysqlConfig);
		this.version = mysqlJdbcContext.queryVersion();
		ArrayList<Map<String, Object>> inconsistentNodes = MysqlUtil.compareMasterSlaveCurrentTime(mysqlConfig, contextMapForMasterSlave);
		if (null != inconsistentNodes && inconsistentNodes.size() == 2) {
			Map<String, Object> node1 = inconsistentNodes.get(0);
			Map<String, Object> node2 = inconsistentNodes.get(1);
			tapLogger.warn(String.format("The time of each node is inconsistent, please check nodes: %s and %s", node1.toString(), node2.toString()));
		}
		if (tapConnectionContext instanceof TapConnectorContext) {
			if (DeployModeEnum.fromString(mysqlConfig.getDeploymentMode()) == DeployModeEnum.MASTER_SLAVE) {
				KVMap<Object> stateMap = ((TapConnectorContext) tapConnectionContext).getStateMap();
				Object masterNode = stateMap.get(MASTER_NODE_KEY);
				if (null != masterNode && null != mysqlConfig.getMasterNode()) {
					if (!masterNode.toString().contains(mysqlConfig.getMasterNode().toString()))
						tapLogger.warn(String.format("The master node has switched, please pay attention to whether the data is consistent, current master node: %s", mysqlConfig.getMasterNode()));
				}
			}
			this.mysqlWriter = new MysqlSqlBatchWriter(mysqlJdbcContext, this::isAlive);
			this.mysqlReader = new MysqlReader(mysqlJdbcContext, tapLogger, this::isAlive);
			this.dbTimeZone = mysqlJdbcContext.queryTimeZone();
			if (mysqlConfig.getOldVersionTimezone()) {
				this.timeZone = dbTimeZone;
			} else {
				this.timeZone = TimeZone.getTimeZone("GMT" + mysqlConfig.getTimezone());
			}
			this.dbZoneId = dbTimeZone.toZoneId();
			this.zoneId = timeZone.toZoneId();
			this.zoneOffsetHour = timeZone.getRawOffset() / 1000 / 60 / 60;
			ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
		}
		fieldDDLHandlers = new BiClassHandlers<>();
		fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
		fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
		fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
		fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
		started.set(true);
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
		mysqlConfig = new AliyunRDSMySQLConfig().load(connectionContext.getConnectionConfig());
		contextMapForMasterSlave = MysqlUtil.buildContextMapForMasterSlave(mysqlConfig);
		MysqlUtil.buildMasterNode(mysqlConfig, contextMapForMasterSlave);
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		connectionOptions.connectionString(mysqlConfig.getConnectionString());
		try (
				MysqlConnectionTest mysqlConnectionTest = new MysqlConnectionTest(mysqlConfig, consumer, connectionOptions)
		) {
			mysqlConnectionTest.testOneByOne();
		}
		return connectionOptions;
	}
}
