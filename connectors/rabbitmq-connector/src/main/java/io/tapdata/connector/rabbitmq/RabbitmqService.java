package io.tapdata.connector.rabbitmq;

import com.rabbitmq.client.*;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.QueueInfo;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.rabbitmq.config.RabbitmqConfig;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RabbitmqService extends AbstractMqService {

    private static final String TAG = RabbitmqService.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private final ConnectionFactory connectionFactory;
    private Connection rabbitmqConnection;
    private static final String RABBITMQ_URL = "http://%s:%s/api";
    protected static final String RABBIT_CONTENT_TYPE_JSON = "application/json";
    protected static final String RABBIT_CONTENT_ENCODE = "utf-8";
    AtomicReference<Throwable> atomicReference = new AtomicReference<>();

    public RabbitmqService(RabbitmqConfig mqConfig) {
        this.mqConfig = mqConfig;
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(mqConfig.getMqHost());
        connectionFactory.setPort(mqConfig.getMqPort());
        connectionFactory.setUsername(mqConfig.getMqUsername());
        connectionFactory.setPassword(mqConfig.getMqPassword());
        connectionFactory.setVirtualHost(mqConfig.getVirtualHost());
    }

    @Override
    public TestItem testConnect() {
        try {
            rabbitmqConnection = connectionFactory.newConnection();
            return new TestItem(MqTestItem.RABBIT_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
        } catch (Throwable t) {
            return new TestItem(MqTestItem.RABBIT_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, t.getMessage());
        }
    }

    @Override
    public ConnectionCheckItem testConnection() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_CONNECTION);
        try {
            rabbitmqConnection.createChannel().close();
            connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

    @Override
    public void init() throws Throwable {
        rabbitmqConnection = connectionFactory.newConnection();
    }

    @Override
    public void close() {
        super.close();
        try {
            if (EmptyKit.isNotNull(rabbitmqConnection)) {
                rabbitmqConnection.close();
            }
        } catch (IOException e) {
            TapLogger.error(TAG, "close connection error", e);
        }
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T destination, TapTable tapTable) throws Exception {
        Channel channel = (Channel) object;
        tapTable.setId((String) destination);
        tapTable.setName((String) destination);
        GetResponse message = channel.basicGet((String) destination, false);
        if (message == null) {
            return new HashMap<>();
        }
        return parse(message.getProps(), message.getBody());
    }

    @Override
    public int countTables() throws Throwable {
        if (EmptyKit.isEmpty(mqConfig.getMqQueueSet())) {
            Client client = new Client(String.format(RABBITMQ_URL, mqConfig.getMqHost(), ((RabbitmqConfig) mqConfig).getApiPort()),
                    mqConfig.getMqUsername(), mqConfig.getMqPassword());
            return client.getQueues().size();
        } else {
            return mqConfig.getMqQueueSet().size();
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        Client client = new Client(String.format(RABBITMQ_URL, mqConfig.getMqHost(), ((RabbitmqConfig) mqConfig).getApiPort()),
                mqConfig.getMqUsername(), mqConfig.getMqPassword());
        Channel channel = rabbitmqConnection.createChannel();
        Set<String> existQueueSet = client.getQueues().stream().map(QueueInfo::getName).collect(Collectors.toSet());
        Set<String> destinationSet = new HashSet<>();
        Set<String> existQueueNameSet = new HashSet<>();
        if (EmptyKit.isEmpty(mqConfig.getMqQueueSet())) {
            destinationSet.addAll(existQueueSet);
        } else {
            //query queue which exists
            for (String queue : existQueueSet) {
                if (mqConfig.getMqQueueSet().contains(queue)) {
                    destinationSet.add(queue);
                    existQueueNameSet.add(queue);
                }
            }
            //create queue which not exists
            Set<String> needCreateQueueSet = mqConfig.getMqQueueSet().stream()
                    .filter(i -> !existQueueNameSet.contains(i)).collect(Collectors.toSet());
            if (EmptyKit.isNotEmpty(needCreateQueueSet)) {
                for (String queue : needCreateQueueSet) {
                    channel.queueDeclare(queue, true, false, false, null);
                    destinationSet.add(queue);
                }
            }
        }
        submitTables(tableSize, consumer, channel, destinationSet);
        channel.close();
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws Throwable {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        Channel channel = rabbitmqConnection.createChannel();
        if (EmptyKit.isNull(channel)) {
            ErrorKit.ignoreAnyError(rabbitmqConnection::close);
            rabbitmqConnection = connectionFactory.newConnection();
            channel = rabbitmqConnection.createChannel();
        }
        if (EmptyKit.isNull(tapTable)) {
            return;
        }
        channel.queueDeclare(tapTable.getId(), true, false, false, null);

        for (TapRecordEvent event : tapRecordEvents) {
            if (null != isAlive && !isAlive.get()) {
                break;
            }
            Map<String, Object> headers = new HashMap<>();
            byte[] body = null;
            MqOp mqOp = MqOp.INSERT;
            if (event instanceof TapInsertRecordEvent) {
                body = jsonParser.toJsonBytes(((TapInsertRecordEvent) event).getAfter());
            } else if (event instanceof TapUpdateRecordEvent) {
                body = jsonParser.toJsonBytes(((TapUpdateRecordEvent) event).getAfter());
                mqOp = MqOp.UPDATE;
            } else if (event instanceof TapDeleteRecordEvent) {
                body = jsonParser.toJsonBytes(((TapDeleteRecordEvent) event).getBefore());
                mqOp = MqOp.DELETE;
            }
            headers.put("mqOp", mqOp.getOp());
            AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                    .contentType(RABBIT_CONTENT_TYPE_JSON)
                    .contentEncoding(RABBIT_CONTENT_ENCODE)
                    .headers(headers)
                    .build();
            try {
                channel.basicPublish("", tapTable.getId(), props, body);
                switch (mqOp) {
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
            } catch (Exception e) {
                listResult.addError(event, e);
            }
        }
        writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        atomicReference.set(null);
        consuming.set(true);
        Channel channel = rabbitmqConnection.createChannel();
        String tableName = tapTable.getId();
        List<TapEvent> list = TapSimplify.list();
        AtomicLong time = new AtomicLong(System.currentTimeMillis());
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    super.handleDelivery(consumerTag, envelope, properties, body);
                    MqOp mqOp = MqOp.fromValue(properties.getHeaders().get("mqOp").toString());
                    Map<String, Object> data = parse(properties,body);//jsonParser.fromJsonBytes(body, Map.class);
                    switch (mqOp) {
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
                    channel.basicAck(envelope.getDeliveryTag(), false);
                    if (list.size() >= eventBatchSize) {
                        List<TapEvent> subList = TapSimplify.list();
                        subList.addAll(list);
                        eventsOffsetConsumer.accept(subList, TapSimplify.list());
                        time.set(System.currentTimeMillis());
                        list.clear();
                    }
                } catch (Exception e) {
                    atomicReference.set(e);
                    throw e;
                }
            }
        };
        channel.queueDeclare(tableName, true, false, false, null);
        channel.basicConsume(tableName, consumer);
        while (consuming.get() && System.currentTimeMillis() - time.get() < 10000) {
            exception();
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                tapLogger.debug(e.getMessage());
            }
        }
        try {
            channel.close();
        } catch (Exception e) {
            tapLogger.warn(e.getMessage());
        }
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
    }

    protected void exception() {
        Throwable throwable = atomicReference.get();
        if (null != throwable) {
            tapLogger.warn(throwable.getMessage(), throwable);
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        atomicReference.set(null);
        Channel channel = rabbitmqConnection.createChannel();
        tableList.forEach(tableName -> {
            List<TapEvent> list = TapSimplify.list();
            DefaultConsumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    try {
                        super.handleDelivery(consumerTag, envelope, properties, body);
                        MqOp mqOp = MqOp.fromValue(properties.getHeaders().get("mqOp").toString());
                        Map<String, Object> data = parse(properties,body);//jsonParser.fromJsonBytes(body, Map.class);
                        switch (mqOp) {
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
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        if (list.size() >= eventBatchSize) {
                            List<TapEvent> subList = TapSimplify.list();
                            subList.addAll(list);
                            eventsOffsetConsumer.accept(subList, TapSimplify.list());
                            list.clear();
                        }
                    } catch (Exception e) {
                        atomicReference.set(e);
                        throw e;
                    }
                }
            };
            try {
                channel.queueDeclare(tableName, true, false, false, null);
                channel.basicConsume(tableName, consumer);
                while (consuming.get()) {
                    exception();
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        tapLogger.debug(e.getMessage());
                    }
                }
                channel.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotEmpty(list)) {
                eventsOffsetConsumer.accept(list, TapSimplify.list());
            }
        });
    }

    public void blockConsume() {
        consuming.compareAndSet(true, false);
    }

    public Map<String, Object> parse(AMQP.BasicProperties props, byte[] body) throws UnsupportedEncodingException {
        String contentType = String.valueOf(props.getContentType()).toLowerCase();
        if (RABBIT_CONTENT_TYPE_JSON.equals(contentType)) {
            String contentEncoding = Optional.ofNullable(props.getContentEncoding()).orElse(RABBIT_CONTENT_ENCODE);
            String bodyString = new String(body, contentEncoding);
            if (bodyString.startsWith("\"")) {
                bodyString = bodyString.substring(1);
            }
            if (bodyString.endsWith("\"")) {
                bodyString = bodyString.substring(0, bodyString.length() - 1);
            }
            return TapSimplify.fromJson(bodyString, Map.class);
        } else {
            try {
                return jsonParser.fromJsonBytes(body, Map.class);
            } catch (Exception e) {
                tapLogger.warn("Can not parse bytes from RabbitMQ, data format mismatch: {}, props: {}", new String(body), TapSimplify.toJson(props));
                throw e;
            }
        }
    }
}
