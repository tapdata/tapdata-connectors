package io.tapdata.connector.kafka;

import com.google.common.collect.Lists;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.MultiThreadUtil.ConcurrentCalculator;
import io.tapdata.connector.kafka.MultiThreadUtil.Concurrents;
import io.tapdata.connector.kafka.MultiThreadUtil.DMLRecordEventConvert;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.*;
import io.tapdata.connector.kafka.util.*;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.exception.StopException;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.common.constant.MqOp.*;

public class KafkaService extends AbstractMqService {

    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private String connectorId;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
	private ScriptEngine produceScriptEngineCustomDDL;
	private ScriptEngine produceScriptEngineCustomDML;
    public static final String scriptEngineName = "graal.js";

    private ConcurrentHashMap<String, Invocable> customParseScriptEngine = new ConcurrentHashMap<>();

	public static final int CONSUME_CUSTOM_MESSAGE_CORE_SIZE = 4;
	private ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");

	private Concurrents<Invocable> customParseConcurrents;

	private ConcurrentCalculator<ConsumerRecord<byte[], byte[]>,TapEvent> customParseCalculator;

	private Concurrents<Invocable> customDmlConcurrents;
	private ConcurrentCalculator<DMLRecordEventConvert, DMLRecordEventConvert> customDmlCalculator;


    Invocable customScriptEngine = null;

    Invocable declareScriptEngine = null;
    public KafkaService() {
	}

    public KafkaService(KafkaConfig mqConfig, Log tapLogger) {
        this.mqConfig = mqConfig;
        this.tapLogger = tapLogger;
        ProducerConfiguration producerConfiguration = new ProducerConfiguration(mqConfig, connectorId);
        try {
            kafkaProducer = new KafkaProducer<>(producerConfiguration.build());
        } catch (Exception e) {
            e.printStackTrace();
            tapLogger.error("Kafka producer error: " + ErrorKit.getLastCause(e).getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    public String getConnectorId() {
        return connectorId;
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) {
        return null;
    }

    @Override
    public TestItem testConnect() {
        if (((KafkaConfig) mqConfig).getKrb5()) {
            try {
                Krb5Util.checkKDCDomainsBase64(((KafkaConfig) mqConfig).getKrb5Conf());
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } catch (Exception e) {
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_FAILED, e.getMessage());
            }
        }
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (Admin admin = new DefaultAdmin(configuration)) {
            if (admin.isClusterConnectable()) {
                return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } else {
                return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "cluster is not connectable");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "when connect to cluster, error occurred " + e.getMessage());
        }
    }

    @Override
    public ConnectionCheckItem testConnection() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_CONNECTION);
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            if (admin.isClusterConnectable()) {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
            } else {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information("cluster is not connectable");
            }
        } catch (Exception e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

	@Override
	public void init() {
		KafkaConfig kafkaConfig = (KafkaConfig) mqConfig;
		Boolean enableCustomParse = kafkaConfig.getEnableCustomParse();
		if (Boolean.TRUE.equals(enableCustomParse)) {
			String enableCustomParseScript = kafkaConfig.getEnableCustomParseScript();
			customParseConcurrents = new Concurrents<Invocable>() {
				@Override
				protected String getInstanceId() {
					String id = String.valueOf(Thread.currentThread().getId());
					return id;
				}

				@Override
				protected Invocable initNewInstance(String instanceId) {
					Invocable customParseEngine = ScriptUtil.getScriptEngine(enableCustomParseScript, scriptEngineName, scriptFactory);
					return customParseEngine;
				}
			};
			customParseCalculator = new ConcurrentCalculator<ConsumerRecord<byte[], byte[]>, TapEvent>(CONSUME_CUSTOM_MESSAGE_CORE_SIZE) {
				@Override
				protected TapEvent performComputation(ConsumerRecord<byte[], byte[]> data) {
					TapEvent process = customParseConcurrents.process(scriptEngine -> makeCustomMessageUseScriptEngine(scriptEngine, data));
					return process;
				}
				@Override
				protected List<TapEvent> performComputation(List<ConsumerRecord<byte[], byte[]>> data) {
					List<TapEvent> process = customParseConcurrents.process(scriptEngine -> {
						List<TapEvent> result = new ArrayList<>();
						for (ConsumerRecord<byte[], byte[]> consumerRecord : data) {
							TapEvent tapEvent = makeCustomMessageUseScriptEngine(scriptEngine, consumerRecord);
							if (null != tapEvent) {
								result.add(tapEvent);
							}
						}
						return result;
					});
					return process;
				}
			};
//			customScriptEngine = ScriptUtil.getScriptEngine(enableCustomParseScript, scriptEngineName, scriptFactory);
		}
		Boolean enableModelDeclare = kafkaConfig.getEnableModelDeclare();
		if (Boolean.TRUE.equals(enableModelDeclare)) {
			declareScriptEngine = ScriptUtil.getScriptEngine(kafkaConfig.getEnableModelDeclareScript(), scriptEngineName, scriptFactory);
			KafkaTapModelDeclare tapModelDeclare = new KafkaTapModelDeclare();
			((ScriptEngine) declareScriptEngine).put("TapModelDeclare", tapModelDeclare);
		}

		if (Boolean.TRUE.equals(kafkaConfig.getEnableScript())) {
			try {
				customDmlConcurrents = new Concurrents<Invocable>() {
					@Override
					protected String getInstanceId() {
						String id = String.valueOf(Thread.currentThread().getId());
						return id;
					}

					@Override
					protected Invocable initNewInstance(String instanceId) {
						Invocable customParseEngine = ScriptUtil.getScriptEngine(kafkaConfig.getScript(), scriptEngineName, scriptFactory);
						return customParseEngine;
					}
				};
				customDmlCalculator = new ConcurrentCalculator<DMLRecordEventConvert, DMLRecordEventConvert>(CONSUME_CUSTOM_MESSAGE_CORE_SIZE) {
					@Override
					protected DMLRecordEventConvert performComputation(DMLRecordEventConvert dmlRecordEventConvert) {
						DMLRecordEventConvert result = customDmlConcurrents.process(scriptEngine -> {
							Collection<String> primaryKeys = dmlRecordEventConvert.getTapTable().primaryKeys(true);
							TapRecordEvent event = dmlRecordEventConvert.getRecordEvent();
							Map<String, Object> jsProcessParam = new HashMap<>();
							Map<String, Map<String, Object>> allData = new HashMap();
							MqOp mqOp = INSERT;
							Map<String, Object> eventInfo = new HashMap<>();
							eventInfo.put("tableId", event.getTableId());
							eventInfo.put("eventInfo", event.getInfo());
							eventInfo.put("referenceTime", event.getReferenceTime());
							jsProcessParam.put("eventInfo", eventInfo);
							Map<String, Object> data;
							if (event instanceof TapInsertRecordEvent) {
								data = ((TapInsertRecordEvent) event).getAfter();
								allData.put("before", new HashMap<String, Object>());
								allData.put("after", data);
							} else if (event instanceof TapUpdateRecordEvent) {
								data = ((TapUpdateRecordEvent) event).getAfter();
								Map<String, Object> before = ((TapUpdateRecordEvent) event).getBefore();
								allData.put("before", null == before ? new HashMap<>() : before);
								allData.put("after", data);
								mqOp = UPDATE;
							} else if (event instanceof TapDeleteRecordEvent) {
								data = ((TapDeleteRecordEvent) event).getBefore();
								allData.put("before", data);
								allData.put("after", new HashMap<String, Object>());
								mqOp = DELETE;
							} else {
								data = new HashMap<>();
							}
							byte[] kafkaMessageKey = getKafkaMessageKey(data, dmlRecordEventConvert.getTapTable());
							dmlRecordEventConvert.setKafkaMessageKey(kafkaMessageKey);
							jsProcessParam.put("data", allData);
							String op = mqOp.getOp();
							Map<String, Object> header = new HashMap();
							header.put("mqOp", op);
							jsProcessParam.put("header", header);
							Object jsConvertResult = ObjectUtils.covertData(executeScript(scriptEngine, "process", jsProcessParam, op, primaryKeys));
							if (jsConvertResult != null) {
								Map<String, Object> jsConvertResultMap = (Map<String, Object>) jsConvertResult;
								jsConvertResultMap.put("op", op);
								dmlRecordEventConvert.setJsConvertResultMap(jsConvertResultMap);
							}
							return dmlRecordEventConvert;
						});
						return result;
					}
                    @Override
                    protected List<DMLRecordEventConvert> performComputation(List<DMLRecordEventConvert> data) {
                        return null;
                    }
                };

//				String script = kafkaConfig.getScript();
//				produceScriptEngineCustomDML = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(scriptEngineName));
//				String buildInMethod = ScriptUtil.initBuildInMethod();
//				String scripts = script + System.lineSeparator() + buildInMethod;
//				produceScriptEngineCustomDML.eval(scripts);
			} catch (Exception e) {
				throw new RuntimeException("Init DML custom parser failed: " + e.getMessage(), e);
			}
		}

		if (Boolean.TRUE.equals(kafkaConfig.getEnableCustomDDLMessage())) {
			try {
				String script = kafkaConfig.getEnableDDLCustomScript();
				produceScriptEngineCustomDDL = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(scriptEngineName));
				String buildInMethod = ScriptUtil.initBuildInMethod();
				String scripts = script + System.lineSeparator() + buildInMethod;
				produceScriptEngineCustomDDL.eval(scripts);
			} catch (ScriptException e) {
				throw new RuntimeException("Init DDL custom parser failed: " + e.getMessage(), e);
			}
		}
	}

    @Override
    public int countTables() throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            Set<String> topicSet = admin.listTopics();
            if (EmptyKit.isEmpty(mqConfig.getMqTopicSet())) {
                return topicSet.size();
            } else {
                return (int) topicSet.stream().filter(topic -> mqConfig.getMqTopicSet().stream().anyMatch(reg -> StringKit.matchReg(topic, reg))).count();
            }
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        Set<String> destinationSet = new HashSet<>();
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            Set<String> existTopicSet = admin.listTopics();
            if (EmptyKit.isEmpty(mqConfig.getMqTopicSet())) {
                destinationSet.addAll(existTopicSet);
            } else {
                //query queue which exists
                for (String topic : existTopicSet) {
                    if (mqConfig.getMqTopicSet().stream().anyMatch(reg -> StringKit.matchReg(topic, reg))) {
                        destinationSet.add(topic);
                    }
                }
            }
        }
        SchemaConfiguration schemaConfiguration = new SchemaConfiguration(((KafkaConfig) mqConfig), connectorId);
        multiThreadDiscoverSchema(new ArrayList<>(destinationSet), tableSize, consumer, schemaConfiguration);
    }

    protected void multiThreadDiscoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration) {
        CopyOnWriteArraySet<List<String>> tableLists = new CopyOnWriteArraySet<>(DbKit.splitToPieces(tables, tableSize));
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(20);
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        try {
            for (int i = 0; i < 20; i++) {
                executorService.submit(() -> {
                    try {
                        List<String> subList;
                        while ((subList = getOutTableList(tableLists)) != null) {
//                            if(((KafkaConfig)mqConfig).getEnableCustomParse()){
//                                submitPageTablesCustomParse(tableSize, consumer, schemaConfiguration, subList);
//                            }else{
                            submitPageTables(tableSize, consumer, schemaConfiguration, subList);
//                            }
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                throw new RuntimeException(throwable.get());
            }
        } finally {
            executorService.shutdown();
        }
    }



    private synchronized List<String> getOutTableList(CopyOnWriteArraySet<List<String>> tableLists) {
        if (EmptyKit.isNotEmpty(tableLists)) {
            List<String> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }

    protected synchronized void syncSchemaSubmit(List<TapTable> tapTables, Consumer<List<TapTable>> consumer) {
        consumer.accept(tapTables);
    }

    public void loadTables(List<String> tableNames, int tableSize, Consumer<List<TapTable>> consumer) {
        SchemaConfiguration schemaConfiguration = new SchemaConfiguration(((KafkaConfig) mqConfig), connectorId);
        multiThreadDiscoverSchema(new ArrayList<>(tableNames), tableSize, consumer, schemaConfiguration);
    }

    protected void submitPageTables(int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration, List<String> destinationSet) {
        List<List<String>> tablesList = Lists.partition(destinationSet, tableSize);
        Map<String, Object> config = schemaConfiguration.build();
        try (
                KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(config)
        ) {
            tablesList.forEach(tables -> {
                List<TapTable> tableList = new ArrayList<>();
                List<String> topics = new ArrayList<>(tables);
                kafkaConsumer.subscribe(topics);
                ConsumerRecords<byte[], byte[]> consumerRecords;

                while (!(consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L))).isEmpty()) {
                    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                        if (!topics.contains(record.topic())) {
                            continue;
                        }
                        Map<String, Object> messageBody;
                        try {
                            messageBody = jsonParser.fromJsonBytes(record.value(), Map.class);
                        } catch (Exception e) {
                            tapLogger.error("topic[{}] value [{}] can not parse to json, ignore...", record.topic(), record.value());
                            continue;
                        }
                        if (messageBody == null) {
                            tapLogger.warn("messageBody not allow null...");
                            continue;
                        }
                        if (messageBody.containsKey("mqOp")) {
                            messageBody = (Map<String, Object>) messageBody.get("data");
                        }
                        if (((KafkaConfig) mqConfig).getEnableCustomParse()) {
                            try{
                                customParseLoadSchema(record, messageBody, tableList);
                            }catch (Throwable t){
                                tapLogger.error(String.format("%s custom Parse topic message error: %s",record.topic(),t.getMessage()),t);
                            }
                        } else {
                            try {
                                TapTable tapTable = new TapTable(record.topic());
                                SCHEMA_PARSER.parse(tapTable, messageBody);
                                tableList.add(tapTable);
                            } catch (Throwable t) {
                                tapLogger.error(String.format("%s parse topic invalid json object: %s", record.topic(), t.getMessage()), t);
                            }
                        }
                        topics.remove(record.topic());
                    }
                    if (EmptyKit.isEmpty(topics)) {
                        break;
                    }
                    kafkaConsumer.subscribe(topics);
                }
                topics.stream().map(TapTable::new).forEach(tableList::add);
//                declareSchemaIfNeed(tableList);
                syncSchemaSubmit(tableList, consumer);
            });
        }
    }

    private void declareSchemaIfNeed(List<TapTable> tableList) {
        if (CollectionUtils.isNotEmpty(tableList)) {
            if (((KafkaConfig) mqConfig).getEnableModelDeclare()) {
                tableList.forEach(tapTable -> {
                    try {
                        declareScriptEngine.invokeFunction("declare", tapTable);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    private void customParseLoadSchema(ConsumerRecord<byte[], byte[]> record, Map<String, Object> messageBody, List<TapTable> tableList) {
        Object event = ObjectUtils.covertData(executeScript(customScriptEngine, "process", messageBody));
        if (null == event) {
            tapLogger.error("event is null");
        }
        TapBaseEvent tapBaseEvent = CustomParseUtil.applyCustomParse((Map<String, Object>) event);
        if (tapBaseEvent instanceof TapRecordEvent) {
            TapTable tapTable = new TapTable(record.topic());
            if (tapBaseEvent instanceof TapInsertRecordEvent) {
                SCHEMA_PARSER.parse(tapTable, ((TapInsertRecordEvent) tapBaseEvent).getAfter());
            } else if (tapBaseEvent instanceof TapUpdateRecordEvent) {
                SCHEMA_PARSER.parse(tapTable, ((TapUpdateRecordEvent) tapBaseEvent).getAfter());
            }
            tableList.add(tapTable);
        }
    }

    public static Object executeScript(ScriptEngine scriptEngine, String function, Object... params) {
        if (scriptEngine != null) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                return invocable.invokeFunction(function, params);
            } catch (StopException e) {
//                TapLogger.info(TAG, "Get data and stop script.");
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
//                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        return null;
    }
    public static Object executeScript(Invocable scriptEngine, String function, Object... params) {
        if (scriptEngine != null) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                return invocable.invokeFunction(function, params);
            } catch (StopException e) {
//                TapLogger.info(TAG, "Get data and stop script.");
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
//                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
        try {
            for (TapRecordEvent event : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                Map<String, Object> data;
                MqOp mqOp = MqOp.INSERT;
                if (event instanceof TapInsertRecordEvent) {
                    data = ((TapInsertRecordEvent) event).getAfter();
                } else if (event instanceof TapUpdateRecordEvent) {
                    data = ((TapUpdateRecordEvent) event).getAfter();
                    mqOp = MqOp.UPDATE;
                } else if (event instanceof TapDeleteRecordEvent) {
                    data = ((TapDeleteRecordEvent) event).getBefore();
                    mqOp = MqOp.DELETE;
                } else {
                    data = new HashMap<>();
                }
                byte[] body = jsonParser.toJsonBytes(data, JsonParser.ToJsonFeature.WriteMapNullValue);
                MqOp finalMqOp = mqOp;
                Callback callback = (metadata, exception) -> {
                    try {
                        if (EmptyKit.isNotNull(exception)) {
                            listResult.addError(event, exception);
                        }
                        switch (finalMqOp) {
                            case INSERT:
                                insert.incrementAndGet();
                                break;
                            case UPDATE:
                                update.incrementAndGet();
                                break;
                            case DELETE:
                                delete.incrementAndGet();
                                break;
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                };
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), getKafkaMessageKey(data, tapTable), body,
                        new RecordHeaders().add("mqOp", mqOp.getOp().getBytes()));
                kafkaProducer.send(producerRecord, callback);
            }
        } catch (RejectedExecutionException e) {
            tapLogger.warn("task stopped, some data produce failed!", e);
        } catch (Exception e) {
            tapLogger.error("produce error, or task interrupted!", e);
        }
        try {
            while (null != isAlive && isAlive.get()) {
                if (countDownLatch.await(500L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            tapLogger.error("error occur when await", e);
        } finally {
            writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
        }
//            this.produce(null,tapRecordEvents,tapTable,writeListResultConsumer,isAlive);
    }

	@Override
	public void produce(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) {
		AtomicLong insert = new AtomicLong(0);
		AtomicLong update = new AtomicLong(0);
		AtomicLong delete = new AtomicLong(0);
		WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
		CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
		try {
			List<DMLRecordEventConvert> dmlRecordEventBeforeConverts = tapRecordEvents.stream().map((event -> {
				DMLRecordEventConvert dmlRecordEventConvert = new DMLRecordEventConvert();
				dmlRecordEventConvert.setRecordEvent(event);
				dmlRecordEventConvert.setTapTable(tapTable);
				return dmlRecordEventConvert;
			})).collect(Collectors.toList());
			List<DMLRecordEventConvert> dmlRecordEventAfterConverts = customDmlCalculator.calc(dmlRecordEventBeforeConverts);
			dmlRecordEventAfterConverts.stream().filter(res-> MapUtils.isNotEmpty(res.getJsConvertResultMap())).forEach(dmlRecordEventConvert->{
				TapRecordEvent event = dmlRecordEventConvert.getRecordEvent();
				Map<String, Object> jsConvertResultMap = dmlRecordEventConvert.getJsConvertResultMap();
				byte[] body = {};
				RecordHeaders recordHeaders = new RecordHeaders();
				if (null == jsConvertResultMap.get("data")) {
					throw new RuntimeException("data cannot be null");
				} else {
					Object obj = jsConvertResultMap.get("data");
					if (obj instanceof Map) {
						Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) jsConvertResultMap.get("data");
						removeIfEmptyInMap(map, "before");
						removeIfEmptyInMap(map, "after");
						body = jsonParser.toJsonBytes(jsConvertResultMap.get("data"), JsonParser.ToJsonFeature.WriteMapNullValue);
					} else {
						body = obj.toString().getBytes();
					}
				}
				String mqOp = MapUtils.getString(jsConvertResultMap, "op");
				if (jsConvertResultMap.containsKey("header")) {
					Object obj = jsConvertResultMap.get("header");
					if (obj instanceof Map) {
						Map<String, Object> head = (Map<String, Object>) jsConvertResultMap.get("header");
						for (String s : head.keySet()) {
							recordHeaders.add(s, head.get(s).toString().getBytes());
						}
					} else {
						throw new RuntimeException("header must be a collection type");
					}
				} else {
					recordHeaders.add("mqOp", mqOp.toString().getBytes());
				}
				MqOp finalMqOp = MqOp.fromValue(mqOp);

				Callback callback = (metadata, exception) -> {
					try {
						if (EmptyKit.isNotNull(exception)) {
							listResult.addError(event, exception);
						}
						switch (finalMqOp) {
							case INSERT:
								insert.incrementAndGet();
								break;
							case UPDATE:
								update.incrementAndGet();
								break;
							case DELETE:
								delete.incrementAndGet();
								break;
						}
					} finally {
						countDownLatch.countDown();
					}
				};
				ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
					null, null, dmlRecordEventConvert.getKafkaMessageKey(), body,
					recordHeaders);
				kafkaProducer.send(producerRecord, callback);
			});

		} catch (RejectedExecutionException e) {
			tapLogger.warn("task stopped, some data produce failed!", e);
		} catch (Exception e) {
			tapLogger.error("produce error, or task interrupted!", e);
		}
		try {
			while (null != isAlive && isAlive.get()) {
				if (countDownLatch.await(200L, TimeUnit.MILLISECONDS)) {
					break;
				}
			}
		} catch (InterruptedException e) {
			tapLogger.error("error occur when await", e);
		} finally {
			writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
		}
	}

	/*
	@Override
	public void produce(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) {
		AtomicLong insert = new AtomicLong(0);
		AtomicLong update = new AtomicLong(0);
		AtomicLong delete = new AtomicLong(0);
		WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
		CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
//		produceScriptEngineCustomDML.put("eventInfo", eventInfo);
		try {


			for (TapRecordEvent event : tapRecordEvents) {
				if (null != isAlive && !isAlive.get()) {
					break;
				}
				Map<String, Object> record = new HashMap<>();
				Map<String, Map<String, Object>> allData = new HashMap();
				Map<String, Object> eventInfo = new HashMap<>();
				eventInfo.put("tableId", event.getTableId());
				eventInfo.put("eventInfo", event.getInfo());
				eventInfo.put("referenceTime", event.getReferenceTime());
				Map<String, Object> data;
				MqOp mqOp = INSERT;
				if (event instanceof TapInsertRecordEvent) {
					data = ((TapInsertRecordEvent) event).getAfter();
					allData.put("before", new HashMap<String, Object>());
					allData.put("after", data);
				} else if (event instanceof TapUpdateRecordEvent) {
					data = ((TapUpdateRecordEvent) event).getAfter();
					Map<String, Object> before = ((TapUpdateRecordEvent) event).getBefore();
					allData.put("before", null == before ? new HashMap<>() : before);
					allData.put("after", data);
					mqOp = UPDATE;
				} else if (event instanceof TapDeleteRecordEvent) {
					data = ((TapDeleteRecordEvent) event).getBefore();
					allData.put("before", data);
					allData.put("after", new HashMap<String, Object>());
					mqOp = DELETE;
				} else {
					data = new HashMap<>();
				}
				byte[] kafkaMessageKey = getKafkaMessageKey(data, tapTable);
				record.put("data", allData);
				String op = mqOp.getOp();
				Map<String, Object> header = new HashMap();
				header.put("mqOp", op);
				record.put("header", header);
				Collection<String> conditionKeys = tapTable.primaryKeys(true);
				Object jsConvertedData = ObjectUtils.covertData(executeScript(produceScriptEngineCustomDML, "process", record, op, conditionKeys));
				byte[] body = {};
				RecordHeaders recordHeaders = new RecordHeaders();
				if (null == jsConvertedData) {
					continue;
				}
				Map<String, Object> res = (Map<String, Object>) jsConvertedData;
				if (null == res.get("data")) {
					throw new RuntimeException("data cannot be null");
				} else {
					Object obj = res.get("data");
					if (obj instanceof Map) {
						Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) res.get("data");
						removeIfEmptyInMap(map, "before");
						removeIfEmptyInMap(map, "after");
						body = jsonParser.toJsonBytes(res.get("data"), JsonParser.ToJsonFeature.WriteMapNullValue);
					} else {
						body = obj.toString().getBytes();
					}
				}
				if (res.containsKey("header")) {
					Object obj = res.get("header");
					if (obj instanceof Map) {
						Map<String, Object> head = (Map<String, Object>) res.get("header");
						for (String s : head.keySet()) {
							recordHeaders.add(s, head.get(s).toString().getBytes());
						}
					} else {
						throw new RuntimeException("header must be a collection type");
					}
				} else {
					recordHeaders.add("mqOp", mqOp.toString().getBytes());
				}

				MqOp finalMqOp = mqOp;
				Callback callback = (metadata, exception) -> {
					try {
						if (EmptyKit.isNotNull(exception)) {
							listResult.addError(event, exception);
						}
						switch (finalMqOp) {
							case INSERT:
								insert.incrementAndGet();
								break;
							case UPDATE:
								update.incrementAndGet();
								break;
							case DELETE:
								delete.incrementAndGet();
								break;
						}
					} finally {
						countDownLatch.countDown();
					}
				};
				ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
					null, System.currentTimeMillis(), kafkaMessageKey, body,
					recordHeaders);
				kafkaProducer.send(producerRecord, callback);
			}
		} catch (RejectedExecutionException e) {
			tapLogger.warn("task stopped, some data produce failed!", e);
		} catch (Exception e) {
			tapLogger.error("produce error, or task interrupted!", e);
		}
		try {
			while (null != isAlive && isAlive.get()) {
				if (countDownLatch.await(500L, TimeUnit.MILLISECONDS)) {
					break;
				}
			}
		} catch (InterruptedException e) {
			tapLogger.error("error occur when await", e);
		} finally {
			writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
		}
	}*/

	private void removeIfEmptyInMap(Map<String, Map<String, Object>> map, String key) {
		if (!map.containsKey(key)) return;
		Map<String, Object> o = map.get(key);
		if (null == o || o.isEmpty()) {
			map.remove(key);
		}
	}


    @Override
    public void produce(TapFieldBaseEvent tapFieldBaseEvent) {
        AtomicReference<Throwable> reference = new AtomicReference<>();
        byte[] body = jsonParser.toJsonBytes(tapFieldBaseEvent);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapFieldBaseEvent.getTableId(),
                null, tapFieldBaseEvent.getTime(), null, body,
                new RecordHeaders()
                        .add("mqOp", MqOp.DDL.getOp().getBytes())
                        .add("eventClass", tapFieldBaseEvent.getClass().getName().getBytes()));
        Callback callback = (metadata, exception) -> reference.set(exception);
        kafkaProducer.send(producerRecord, callback);
        if (EmptyKit.isNotNull(reference.get())) {
            throw new RuntimeException(reference.get());
        }
    }

    private byte[] getKafkaMessageKey(Map<String, Object> data, TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
            return null;
        } else {
            return jsonParser.toJsonBytes(tapTable.primaryKeys(true).stream().map(key -> String.valueOf(data.get(key))).collect(Collectors.joining("_")));
        }
    }

	@Override
	public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
		consuming.set(true);
		List<TapEvent> list = TapSimplify.list();
		String tableName = tapTable.getId();
		ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KafkaConfig) mqConfig), connectorId, true);
		KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfiguration.build());
		kafkaConsumer.subscribe(Collections.singleton(tapTable.getId()));
		while (consuming.get()) {
			ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(6L));
			if (consumerRecords.isEmpty()) {
				break;
			}
			List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();
			if (((KafkaConfig) mqConfig).getEnableCustomParse()) {
				for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
					consumerRecordList.add(consumerRecord);
				}
				try {
					List<TapEvent> result = customParseCalculator.calc(consumerRecordList);
					for (TapEvent tapEvent : result) {
						list.add(tapEvent);
					}
					if (list.size() >= eventBatchSize) {
						eventsOffsetConsumer.accept(list, TapSimplify.list());
						list = TapSimplify.list();
					}
				} catch (Exception e) {
					new RuntimeException(e);
				}
			} else {
				for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
					if (((KafkaConfig) mqConfig).getEnableCustomParse()) {


//					makeCustomMessage(consumerRecord, list, tableName);
					} else {
						makeMessage(consumerRecord, list, tableName);
					}
					if (list.size() >= eventBatchSize) {
						eventsOffsetConsumer.accept(list, TapSimplify.list());
						list = TapSimplify.list();
					}
				}
			}
		}
		kafkaConsumer.close();
		if (EmptyKit.isNotEmpty(list)) {
			eventsOffsetConsumer.accept(list, TapSimplify.list());
		}
	}

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        consuming.set(true);
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KafkaConfig) mqConfig), connectorId, true);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfiguration.build());

        kafkaConsumer.subscribe(tableList);
        List<TapEvent> list = TapSimplify.list();

        while (consuming.get()) {
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                if(((KafkaConfig) mqConfig).getEnableCustomParse()){
                    makeCustomMessage(consumerRecord,list,consumerRecord.topic());
                }else{
                    makeMessage(consumerRecord, list, consumerRecord.topic());
                }
                if (list.size() >= eventBatchSize) {
                    eventsOffsetConsumer.accept(list, TapSimplify.list());
                    list = TapSimplify.list();
                }
            }
        }
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
        kafkaConsumer.close();
    }
	private TapEvent makeCustomMessageUseScriptEngine(Invocable customScriptEngine,ConsumerRecord<byte[], byte[]> consumerRecord) {
		String threadName = Thread.currentThread().getName();
//        customParseScriptEngine.computeIfAbsent(threadName, (k) -> {
////            return ScriptUtil.getScriptEngine(enableCustomParseScript, scriptEngineName, scriptFactory);
//        });
//		Invocable scriptEngine = customParseScriptEngine.computeIfAbsent(threadName, (k) -> {
//			return ScriptUtil.getScriptEngine(((KafkaConfig) mqConfig).getEnableCustomParseScript(), scriptEngineName, scriptFactory);
//		});
		Map<String, Object> messageBody = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
		Object data = ObjectUtils.covertData(executeScript(customScriptEngine, "process", messageBody));
		if (null == data) return null;
		TapBaseEvent tapBaseEvent = CustomParseUtil.applyCustomParse((Map<String, Object>) data);
		if (tapBaseEvent instanceof TapFieldBaseEvent) {
			TapFieldBaseEvent tapFieldBaseEvent = (TapFieldBaseEvent) tapBaseEvent;
		} else if (tapBaseEvent instanceof TapInsertRecordEvent) {
			TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) tapBaseEvent;
//			tapInsertRecordEvent.table(tableName);
		} else if (tapBaseEvent instanceof TapUpdateRecordEvent) {
			TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) tapBaseEvent;
//			tapUpdateRecordEvent.table(tableName);
		} else {
			TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) tapBaseEvent;
//			tapDeleteRecordEvent.table(tableName);
		}
		return tapBaseEvent;
	}

    private void makeCustomMessage(ConsumerRecord<byte[], byte[]> consumerRecord, List<TapEvent> list, String tableName) {
        String threadName = Thread.currentThread().getName();
//        customParseScriptEngine.computeIfAbsent(threadName, (k) -> {
////            return ScriptUtil.getScriptEngine(enableCustomParseScript, scriptEngineName, scriptFactory);
//        });
//		ScriptUtil.getScriptEngine(((KafkaConfig) mqConfig).getEnableCustomParseScript(), scriptEngineName, scriptFactory);
        Map<String, Object> messageBody = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
        Object data = ObjectUtils.covertData(executeScript(customScriptEngine, "process", messageBody));
        if (null == data) return;
			TapBaseEvent tapBaseEvent = CustomParseUtil.applyCustomParse((Map<String, Object>) data);
			if (tapBaseEvent instanceof TapFieldBaseEvent) {
            TapFieldBaseEvent tapFieldBaseEvent = (TapFieldBaseEvent) tapBaseEvent;
            list.add(tapFieldBaseEvent);
        } else if (tapBaseEvent instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) tapBaseEvent;
            tapInsertRecordEvent.table(tableName);
            list.add(tapInsertRecordEvent);
        } else if (tapBaseEvent instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) tapBaseEvent;
            tapUpdateRecordEvent.table(tableName);
            list.add(tapUpdateRecordEvent);
        } else {
            TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) tapBaseEvent;
            tapDeleteRecordEvent.table(tableName);
            list.add(tapDeleteRecordEvent);
        }
    }
    private void makeMessage(ConsumerRecord<byte[], byte[]> consumerRecord, List<TapEvent> list, String tableName) {
        AtomicReference<String> mqOpReference = new AtomicReference<>();
        mqOpReference.set(INSERT.getOp());
        consumerRecord.headers().headers("mqOp").forEach(header -> mqOpReference.set(new String(header.value())));
        if (MqOp.fromValue(mqOpReference.get()) == MqOp.DDL) {
            consumerRecord.headers().headers("eventClass").forEach(eventClass -> {
                TapFieldBaseEvent tapFieldBaseEvent;
                try {
                    tapFieldBaseEvent = (TapFieldBaseEvent) jsonParser.fromJsonBytes(consumerRecord.value(), Class.forName(new String(eventClass.value())));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                list.add(tapFieldBaseEvent);
            });
        } else {
            Map<String, Object> data = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
            switch (MqOp.fromValue(mqOpReference.get())) {
                case INSERT:
                    list.add(new TapInsertRecordEvent().init().table(tableName).after(data).referenceTime(System.currentTimeMillis()));
                    break;
                case UPDATE:
                    list.add(new TapUpdateRecordEvent().init().table(tableName).after(data).referenceTime(System.currentTimeMillis()));
                    break;
                case DELETE:
                    list.add(new TapDeleteRecordEvent().init().table(tableName).before(data).referenceTime(System.currentTimeMillis()));
                    break;
            }
        }
    }

    @Override
    public void close() {
        super.close();
        if (EmptyKit.isNotNull(kafkaProducer)) {
            kafkaProducer.close();
        }
    }

    public void produceDDLRecord(TapFieldBaseEvent tapFieldBaseEvent) {
        AtomicReference<Throwable> reference = new AtomicReference<>();
        Map<String, Object> data = getDDLRecordData(tapFieldBaseEvent);
        byte[] body = jsonParser.toJsonBytes(data, JsonParser.ToJsonFeature.WriteMapNullValue);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapFieldBaseEvent.getTableId(),
                null, tapFieldBaseEvent.getTime(), null, body,
                new RecordHeaders()
                        .add("mqOp", MqOp.DDL.getOp().getBytes())
                        .add("eventClass", tapFieldBaseEvent.getClass().getName().getBytes()));
        Callback callback = (metadata, exception) -> reference.set(exception);
        kafkaProducer.send(producerRecord, callback);
        if (EmptyKit.isNotNull(reference.get())) {
            throw new RuntimeException(reference.get());
        }
    }

    public void produceCustomDDLRecord(TapFieldBaseEvent tapFieldBaseEvent) {
        AtomicReference<Throwable> reference = new AtomicReference<>();
        Map<String, Object> ddlRecordData = getDDLRecordData(tapFieldBaseEvent);
        Object jsRecordData = ObjectUtils.covertData(executeScript(produceScriptEngineCustomDDL, "process", ddlRecordData));
        if (null == jsRecordData) {
            return;
        }
        Map<String, Object> jsRecordDataMap = (Map<String, Object>) jsRecordData;
        byte[] body = jsonParser.toJsonBytes(jsRecordDataMap, JsonParser.ToJsonFeature.WriteMapNullValue);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapFieldBaseEvent.getTableId(),
                null, tapFieldBaseEvent.getTime(), null, body,
                new RecordHeaders());
        Callback callback = (metadata, exception) -> reference.set(exception);
        kafkaProducer.send(producerRecord, callback);
        if (EmptyKit.isNotNull(reference.get())) {
            throw new RuntimeException(reference.get());
        }
    }

    private static Map<String, Object> getDDLRecordData(TapFieldBaseEvent tapFieldBaseEvent) {
        Map<String, Object> data = new HashMap<>();
        data.put("referenceTime", tapFieldBaseEvent.getReferenceTime());
        data.put("time", tapFieldBaseEvent.getTime());
        data.put("type", tapFieldBaseEvent.getType());
        data.put("ddl", tapFieldBaseEvent.getOriginDDL());
        data.put("tableId", tapFieldBaseEvent.getTableId());
        if (tapFieldBaseEvent instanceof TapNewFieldEvent) {
            TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
            data.put("newFields", tapNewFieldEvent.getNewFields());
        } else if (tapFieldBaseEvent instanceof TapAlterFieldNameEvent) {
            TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
            data.put("nameChange", tapAlterFieldNameEvent.getNameChange());
        } else if (tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent) {
            TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
            data.put("fieldName", tapAlterFieldAttributesEvent.getFieldName());
            data.put("dataTypeChange", tapAlterFieldAttributesEvent.getDataTypeChange());
            data.put("checkChange", tapAlterFieldAttributesEvent.getCheckChange());
            data.put("constraintChange", tapAlterFieldAttributesEvent.getConstraintChange());
            data.put("nullableChange", tapAlterFieldAttributesEvent.getNullableChange());
            data.put("commentChange", tapAlterFieldAttributesEvent.getCommentChange());
            data.put("defaultChange", tapAlterFieldAttributesEvent.getDefaultChange());
            data.put("primaryChange", tapAlterFieldAttributesEvent.getPrimaryChange());
        } else if (tapFieldBaseEvent instanceof TapDropFieldEvent) {
            TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
            data.put("fieldName", tapDropFieldEvent.getFieldName());
        }
        return data;
    }
}
