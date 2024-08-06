package io.tapdata.quickapi;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.APIFactoryImpl;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.quickapi.common.QuickApiConfig;
import io.tapdata.quickapi.server.QuickAPIResponseInterceptor;
import io.tapdata.quickapi.server.TestQuickApi;
import io.tapdata.quickapi.server.enums.QuickApiTestItem;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class QuickApiConnector extends ConnectorBase {
	private static final String TAG = QuickApiConnector.class.getSimpleName();
	private final Object streamReadLock = new Object();

	private QuickApiConfig config;
	private APIInvoker invoker;
	private APIFactory apiFactory;
	private Map<String,Object> apiParam = new HashMap<>();

	private AtomicBoolean task = new AtomicBoolean(true);

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		config = QuickApiConfig.create();
		if (Objects.nonNull(connectionConfig)) {
			String apiType = connectionConfig.getString("apiType");
			if (Objects.isNull(apiType)) apiType = "POST_MAN";
			String jsonTxt = connectionConfig.getString("jsonTxt");
			if (Objects.isNull(jsonTxt)){
				TapLogger.error(TAG,"API JSON must be not null or not empty. ");
			}
			try {
				toJson(jsonTxt);
			}catch (Exception e){
				TapLogger.error(TAG,"API JSON only JSON format. ");
			}
			String expireStatus = connectionConfig.getString("expireStatus");
			String tokenParams = connectionConfig.getString("tokenParams");
			Boolean autoSchema = Boolean.TRUE.equals(connectionConfig.get("autoSchema"));
			String sampleData = String.valueOf(connectionConfig.get("sampleData"));
			config.apiConfig(apiType)
					.jsonTxt(jsonTxt)
					.expireStatus(expireStatus)
					.tokenParams(tokenParams)
					.autoSchema(autoSchema)
					.sampleData(sampleData);
			apiFactory = new APIFactoryImpl();
			invoker = apiFactory.loadAPI(jsonTxt, apiParam);
			invoker.setAPIResponseInterceptor(QuickAPIResponseInterceptor.create(config,invoker));
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		this.task.set(false);
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		if(Objects.nonNull(connectorFunctions)) {
			connectorFunctions.supportBatchCount(this::batchCount)
					.supportBatchRead(this::batchRead)
					.supportTimestampToStreamOffset(this::timestampToStreamOffset)
					.supportErrorHandleFunction(this::errorHandle);
		}else{
			TapLogger.error(TAG,"ConnectorFunctions must be not null or not be empty. ");
		}
	}

	private void streamRead(TapConnectorContext context, List<String> strings, Object o, int i, StreamReadConsumer streamReadConsumer) {
		TapLogger.info(TAG,"QuickAPIConnector does not support StreamRead at the moment. Please manually set the task to incremental only or wait 3 seconds for the task to enter the completion state.");
		try {
			this.wait(3000);
		} catch (InterruptedException ignored) {
		}
	}


	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		return Objects.isNull(time)?System.currentTimeMillis():time;
	}

	public void batchRead(TapConnectorContext context,
						  TapTable table,
						  Object offset,
						  int batchCount,
						  BiConsumer<List<TapEvent>, Object> consumer){
		invoker.pageStage(context,table,offset,batchCount,task,consumer);
	}
	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		return 0L;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		List<String> schema = invoker.tables();
		List<TapTable> tableList = new ArrayList<>();
		boolean autoSchema = Boolean.TRUE.equals(config.getAutoSchema());
		String sampleData = config.getSampleData();
		if (!autoSchema) {
			Map<String, Object> map = new HashMap<>();
			try {
				map = fromJson(sampleData, Map.class);
			} catch (Exception e) {
				connectionContext.getLog().error("Falied to load schema sample data, message: {}, data: ", e.getMessage(), sampleData, e);
			}
			Map<String, Object> finalMap = map;
			schema.stream().filter(Objects::nonNull).forEach(name -> {
				Object data = finalMap.get(name);
				TapTable table = table(name, name);
				LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
				table.setNameFieldMap(fields);
				if (data instanceof Map) {
					analyseSample((Map<String, Object>) data, table);
				}
				tableList.add(table);
			});
		} else {
			schema.stream().filter(Objects::nonNull).forEach(name -> {
				TapTable table = table(name, name)
						.defaultPrimaryKeys(new ArrayList<>());
				LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
				table.setNameFieldMap(fields);
				invoker.mockData(connectionContext, table, new Object(), 100, task, 1, (events, t) -> {
					if (null != events && !events.isEmpty()) {
						events.stream().filter(Objects::nonNull).forEach(e -> {
							Map<String, Object> data = null;
							if (e instanceof TapInsertRecordEvent) {
								data = ((TapInsertRecordEvent) e).getAfter();
							} else if (e instanceof TapUpdateRecordEvent) {
								data = ((TapUpdateRecordEvent) e).getAfter();
							} else if (e instanceof TapDeleteRecordEvent) {
								data = ((TapDeleteRecordEvent) e).getBefore();
							}
							analyseSample(data, table);
						});
					}
				});
				tableList.add(table);
			});
		}
		consumer.accept(tableList);
	}

	protected void analyseSample(Map<String, Object> data, TapTable table) {
		if (null != data && !data.isEmpty()) {
			Map<String, Object> finalData = data;
			data.keySet().stream().filter(k -> !table.getNameFieldMap().containsKey(k)).forEach(k -> {
				TapField field = null;
				Object value = finalData.get(k);
				if (null == value) {
					field = new TapField().name(k).tapType(tapRaw()).dataType("Object");
				} else if (value instanceof Number) {
					field = new TapField().name(k).tapType(tapNumber()).dataType("Number");
				} else if (value instanceof Date) {
					field = new TapField().name(k).tapType(tapDate()).dataType("Date");
				} else if (value instanceof DateTime) {
					field = new TapField().name(k).tapType(tapDateTime()).dataType("DateTime");
				} else if (value instanceof String) {
					field = new TapField().name(k).tapType(tapString()).dataType("String");
				} else if (value instanceof Map) {
					field = new TapField().name(k).tapType(tapMap()).dataType("Object");
				} else if (value instanceof Collection || value.getClass().isArray()) {
					field = new TapField().name(k).tapType(tapArray()).dataType("Array");
				} else if (value instanceof Boolean) {
					field = new TapField().name(k).tapType(tapBoolean()).dataType("Boolean");
				} else {
					field = new TapField().name(k).tapType(tapRaw()).dataType("Object");
				}
				table.add(field);
			});
		}
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		TestQuickApi testQuickApi = null;
		try {
			testQuickApi = TestQuickApi.create(connectionContext);
			consumer.accept(testItem(QuickApiTestItem.TEST_PARAM.testName(), TestItem.RESULT_SUCCESSFULLY));
		}catch (Exception e){
			consumer.accept(testItem(QuickApiTestItem.TEST_PARAM.testName(),TestItem.RESULT_FAILED,e.getMessage()));
		}
		TestItem testTapTableTag = testQuickApi.testTapTableTag();
		consumer.accept(testTapTableTag);
		if (Objects.isNull(testTapTableTag) || Objects.equals(testTapTableTag.getResult(),TestItem.RESULT_FAILED)){
			return connectionOptions;
		}

		TestItem testTokenConfig = testQuickApi.testTokenConfig();
		consumer.accept(testTokenConfig);
		if (Objects.isNull(testTokenConfig) || Objects.equals(testTokenConfig.getResult(),TestItem.RESULT_FAILED)){
			return connectionOptions;
		}

		List<TestItem> testItem = testQuickApi.testApi();
		Optional.ofNullable(testItem).ifPresent(test->test.forEach(consumer));
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		List<String> schema = invoker.tables();
		if (Objects.isNull(schema)) return 0;
		return schema.size();
	}
}
