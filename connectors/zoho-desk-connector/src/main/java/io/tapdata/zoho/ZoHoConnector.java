package io.tapdata.zoho;


import cn.hutool.core.date.DateUtil;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.zoho.webHook.EventBaseEntity;
import io.tapdata.zoho.service.zoho.webHook.WebHookEvent;
import io.tapdata.zoho.service.command.CommandMode;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.loader.ZoHoConnectionTest;
import io.tapdata.zoho.service.zoho.loader.ZoHoStarter;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.service.zoho.impl.SchemaLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/***
 * 1000.dbf93e07349ca3059401bb35486b2bfb.90a94c1a7f08c71934ab53ef1b9e68a8  14:35
 * 手动-添加client
 * 手动-获得clientId,clientSecret
 * 手动-生成Code可设置3-10分钟内有效
 * api获取-clientId+clientSecret+code 获取 asscessToken(一小时内有效，最多30个，每超过就替换最先生成的)，refresh_token（一直有效，知道重新生成，用户获取asscessToken）
 * api获取-clientId+clientSecret+refresh_token 获取 asscessToken(一小时内有效，最多30个，每超过就替换最先生成的)
 *
 *Desk.tickets.ALL,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE
 * Client ID                     1000.RXERF0BIW3RBP7NOJMK615YT9ATRFB
 * Client Secret                 2d5d8f1518a0232cfa33ff45b8ac9566d9c5344cc5
 * {
 *     "access_token": "1000.99586de7eb697435059f3289adc2b138.236b7f9bd27dc60199d2514c128087a7",
 *     "refresh_token": "1000.a664d08e653ce402c62b609f7ab6051a.bf5b49d68b7fc8428561b304ef1f4874",
 *     "api_domain": "https://www.zohoapis.com.cn",
 *     "token_type": "Bearer",
 *     "expires_in": 3600
 * }
 * */

@TapConnectorClass("spec.json")
public class ZoHoConnector extends ConnectorBase {
	private static final String TAG = ZoHoConnector.class.getSimpleName();
	//ZoHo ticket page size 1~100,
	private static final int BATCH_READ_MAX_PAGE_SIZE = 100;

	@Override
	public void onStart(TapConnectionContext connectionContext) {

	}

	@Override
	public void onStop(TapConnectionContext connectionContext) {
		synchronized (this) {
			this.notify();
		}
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead)
				.supportBatchCount(this::batchCount)
				.supportTimestampToStreamOffset(this::timestampToStreamOffset)
				.supportRawDataCallbackFilterFunctionV2(this::rawDataCallbackFilterFunction)
				.supportCommandCallbackFunction(this::handleCommand)
				.supportErrorHandleFunction(this::errorHandle)
		;
	}

	private CommandResult handleCommand(TapConnectionContext tapConnectionContext,  CommandInfo commandInfo) {
		return CommandMode.getInstanceByName(tapConnectionContext,commandInfo);
	}

	private List<TapEvent> rawDataCallbackFilterFunction(TapConnectorContext connectorContext, List<String> tables, Map<String, Object> eventData){
		if (Checker.isEmpty(eventData)){
			TapLogger.debug(TAG,"WebHook of ZoHo patch body is empty, Data callback has been over.");
			return null;
		}
		Object listObj = eventData.get("array");
		if (Checker.isEmpty(listObj) || !(listObj instanceof Collection)){
			TapLogger.debug(TAG,"WebHook of ZoHo patch body is empty or not Collection, Data callback has been over.");
			return null;
		}
		List<Map<String,Object>> dataEventList = (List<Map<String, Object>>)listObj;
		final List<TapEvent>[] events = new List[]{new ArrayList<>()};
		//@TODO BiConsumer<List<TapEvent>, Object> consumer;
		//@TODO 获取筛选条件
		ZoHoStarter zoHoStarter = new ZoHoStarter(connectorContext);
		ContextConfig contextConfig = zoHoStarter.veryContextConfigAndNodeConfig();
		TapConnectionContext context = zoHoStarter.getContext();
		String modeName = contextConfig.connectionMode();
		ConnectionMode instance = ConnectionMode.getInstanceByName(context, modeName);

		if (null == instance){
			throw new CoreException("Connection Mode must be not empty or not null.");
		}
		dataEventList.forEach(eventMap->{
			Object eventTypeObj = eventMap.get("eventType");
			if (Checker.isEmpty(eventTypeObj)) return;
			WebHookEvent event = WebHookEvent.event(String.valueOf(eventTypeObj));
			String table = event.getEventTable();
			if (Checker.isEmpty(table)) return;

			if (!tables.contains(table)) return;
			EventBaseEntity instanceByEventType = EventBaseEntity.getInstanceByEventType(eventMap);
			if (Checker.isEmpty(instanceByEventType)){
				TapLogger.debug(TAG,"An event type with unknown origin was found and cannot be processed .");
				return;
			}
			events[0].add(instanceByEventType.outputTapEvent(table,instance));
			TapLogger.debug(TAG,"From WebHook, ZoHo completed a event [{}] for [{}] table: event data is - {}",instanceByEventType.tapEventType(),table,eventMap);
		});
		return events[0].isEmpty()?null:events[0];
	}

	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		long date = time != null ? time: System.currentTimeMillis();
		List<Schema> schemas = Schemas.allSupportSchemas();
		if (null == schemas || schemas.isEmpty()) {
			return ZoHoOffset.create(new HashMap<>());
		}
		HashMap<String, Long> map = new HashMap<>();
		schemas.forEach(schema -> map.put(schema.schemaName(), date));
		return ZoHoOffset.create(map);
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		String modeName = connectionContext.getConnectionConfig().getString("connectionMode");
		ConnectionMode connectionMode = ConnectionMode.getInstanceByName(connectionContext, modeName);
		if (null == connectionMode){
			throw new CoreException("Connection Mode is not empty or not null.");
		}
		List<TapTable> tapTables = connectionMode.discoverSchema(tables, tableSize);
		if (null != tapTables && !tapTables.isEmpty()){
			consumer.accept(tapTables);
		}
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		ZoHoConnectionTest testConnection= ZoHoConnectionTest.create(connectionContext);
		TestItem testItem = testConnection.testToken();
		consumer.accept(testItem);
		return connectionOptions;
	}


	private void batchRead(
			TapConnectorContext connectorContext,
			TapTable table,
			Object offset,
			int batchCount,
			BiConsumer<List<TapEvent>, Object> consumer) {
		if (Checker.isEmpty(table) || Checker.isEmpty(connectorContext)) return ;
		SchemaLoader loader = SchemaLoader.loader(table.getId(),this);
		if (Checker.isNotEmpty(loader)){
			loader.configSchema(connectorContext).batchRead(ZoHoOffset.from(offset),batchCount,consumer);
			loader.out();
		}
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		if (Checker.isEmpty(tapTable) || Checker.isEmpty(tapConnectorContext)) return 0;
		SchemaLoader loader = SchemaLoader.loader(tapTable.getId(),this);
		if (Checker.isNotEmpty(loader)){
			long count = loader.configSchema(tapConnectorContext).batchCount();
			loader.out();
			return count;
		}
		return 0;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		//check how many projects
		List<Schema> schemas = Schemas.allSupportSchemas();
		return (null == schemas || schemas.isEmpty())?0:schemas.size();
	}

	public void read(TapConnectorContext nodeContext,
					 int readSize,
					 Object offsetState,
					 BiConsumer<List<TapEvent>, Object> consumer,
					 String table ){
		TicketLoader ticketLoader = TicketLoader.create(nodeContext);
		final List<TapEvent>[] events = new List[]{new ArrayList<>()};
		int pageSize = Math.min(readSize, this.BATCH_READ_MAX_PAGE_SIZE);
		HttpEntity tickPageParam = ticketLoader.getTickPageParam()
				.build("limit", pageSize);//分页数
		int fromPageIndex = 1;//从第几个工单开始分页
		String modeName = nodeContext.getConnectionConfig().getString("connectionMode");
		ConnectionMode connectionMode = ConnectionMode.getInstanceByName(nodeContext, modeName);
		if (null == connectionMode){
			throw new CoreException("Connection Mode is not empty or not null.");
		}
		while (isAlive()){
			tickPageParam.build("from", fromPageIndex);
			List<Map<String, Object>> list = ticketLoader.list(tickPageParam);
			if (Checker.isNotEmpty(list) && !list.isEmpty()){
				fromPageIndex += pageSize;
				list.stream().filter(Objects::nonNull).forEach(ticket->{
					Map<String, Object> oneTicket = connectionMode.attributeAssignment(ticket,table,ticketLoader);
					if (Checker.isNotEmpty(oneTicket) && !oneTicket.isEmpty()){
						Object modifiedTimeObj = oneTicket.get("modifiedTime");
						long referenceTime = System.currentTimeMillis();
						if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
							String referenceTimeStr = (String) modifiedTimeObj;
							referenceTime = DateUtil.parse(
									referenceTimeStr.replaceAll("Z", "").replaceAll("T", " "),
									"yyyy-MM-dd HH:mm:ss.SSS").getTime();
							((ZoHoOffset) offsetState).getTableUpdateTimeMap().put(table, referenceTime);
						}
						events[0].add(insertRecordEvent(oneTicket,table).referenceTime(referenceTime));
						if (events[0].size() == readSize){
							consumer.accept(events[0], offsetState);
							events[0] = new ArrayList<>();
						}
					}
				});
				if (!events[0].isEmpty()){
					consumer.accept(events[0], offsetState);
				}
			}else {
				break;
			}
		}
	}
}
