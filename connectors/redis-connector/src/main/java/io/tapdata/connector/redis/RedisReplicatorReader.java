package io.tapdata.connector.redis;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.impl.PingCommand;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.parser.DefaultDumpValueParser;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import com.moilioncircle.redis.replicator.util.ByteArraySet;
import io.tapdata.connector.redis.bean.ClusterNode;
import io.tapdata.connector.redis.bean.ZSetElement;
import io.tapdata.connector.redis.constant.DeployModeEnum;
import io.tapdata.connector.redis.sentinel.RedisSentinelReplicator;
import io.tapdata.connector.redis.sentinel.RedisSentinelURI;
import io.tapdata.connector.redis.util.RedisUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.control.StopEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class RedisReplicatorReader implements AutoCloseable {

    private final RedisConfig redisConfig;
    private final RedisContext redisContext;
    private final String connectorId;
    private final LinkedBlockingQueue<OffsetRedisEvent> eventQueue = new LinkedBlockingQueue<>(5000);
    private final static String INIT_TABLE_NAME = "tapdata";
    private final byte[] publishBytes = "PUBLISH".getBytes(StandardCharsets.UTF_8);
    private final byte[] helloBytes = "__sentinel__:hello".getBytes(StandardCharsets.UTF_8);
    private List<ClusterNode> clusterNodes;
    private ExecutorService executorService;
    private final AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
    private Map<HostAndPort, RedisOffset> offsetMap = new ConcurrentHashMap<>();
    private final Map<String, HostAndPort> replIdMap = new ConcurrentHashMap<>();

    public RedisReplicatorReader(RedisContext redisContext, String connectorId) {
        this.redisContext = redisContext;
        this.redisConfig = redisContext.getRedisConfig();
        this.connectorId = connectorId;
        init();
    }

    private void init() {
        if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.CLUSTER) {
            clusterNodes = generateClusterNodes();
            executorService = Executors.newFixedThreadPool(clusterNodes.size());
        } else {
            clusterNodes = new ArrayList<>();
            executorService = Executors.newFixedThreadPool(1);
        }
    }

    private List<ClusterNode> generateClusterNodes() {
        List<ClusterNode> res = new ArrayList<>();
        try (
                CommonJedis jedis = redisContext.getJedis()
        ) {
            jedis.sendCommand(Protocol.Command.CLUSTER, "SLOTS");
            List<Object> nodes = (List) jedis.sendCommand(Protocol.Command.CLUSTER, "SLOTS");
            nodes.forEach(v -> {
                ClusterNode clusterNode = new ClusterNode();
                List<Object> info = (List) v;
                Iterator<Object> iterator = info.iterator();
                int index = 0;
                while (iterator.hasNext()) {
                    if (index == 0) {
                        clusterNode.setBeginSlot((Long) iterator.next());
                    } else if (index == 1) {
                        clusterNode.setEndSlot((Long) iterator.next());
                    } else if (index == 2) {
                        List<Object> nodeInfo = (List) iterator.next();
                        String host = new String((byte[]) nodeInfo.get(0));
                        int port = ((Long) nodeInfo.get(1)).intValue();
                        clusterNode.setMaster(new HostAndPort(host, port));
                    } else {
                        List<Object> nodeInfo = (List) iterator.next();
                        String host = new String((byte[]) nodeInfo.get(0));
                        int port = ((Long) nodeInfo.get(1)).intValue();
                        clusterNode.addSlave(new HostAndPort(host, port));
                    }
                    index++;
                }
                res.add(clusterNode);
            });
        }
        return res;
    }

    private void startRdbReplicatorThread(String table, CountDownLatch countDownLatch, HostAndPort node, Supplier<Boolean> isAlive) {
        try (
                Replicator redisReplicator = generateReplicator(null, node)
        ) {
            RedisUtil.dress(redisReplicator);
            AtomicReference<String> replId = new AtomicReference<>();
            DefaultDumpValueParser parser = new DefaultDumpValueParser(redisReplicator);
            redisReplicator.addEventListener((replicator, event) -> {
                if (!isAlive.get()) {
                    EmptyKit.closeAsyncQuietly(redisReplicator);
                    return;
                }
                if (event instanceof PreCommandSyncEvent) {
                    enqueueEvent(new StopEvent(), new RedisOffset(replId.get(), ((PreCommandSyncEvent) event).getContext().getOffsets().getV2()));
                    EmptyKit.closeAsyncQuietly(redisReplicator);
                    return;
                }
                try {
                    if (event instanceof DumpKeyValuePair) {
                        DumpKeyValuePair dumpEvent = (DumpKeyValuePair) event;
                        if (dumpEvent.getDb().getDbNumber() != redisConfig.getDatabase()) {
                            return;
                        }
                        Map<String, Object> after = TapSimplify.map(TapSimplify.entry("redis_key", new String(dumpEvent.getKey())));
                        if (dumpEvent.getValue().length > 10240) {
                            after.put("redis_value", "value too large, check info");
                        } else {
                            after.put("redis_value", dumpValueToJson(parser.parse(dumpEvent).getValue()));
                        }
                        TapRecordEvent recordEvent = new TapInsertRecordEvent().init().table(table).after(after);
                        recordEvent.addInfo("redis_event", dumpEvent);
                        enqueueEvent(recordEvent, null);
                    } else if (event instanceof AuxField) {
                        AuxField auxField = (AuxField) event;
                        if ("repl-id".equals(auxField.getAuxKey())) {
                            replId.set(auxField.getAuxValue());
                            replIdMap.put(replId.get(), node);
                        }
                    }
                } catch (Exception e) {
                    exceptionRef.set(e);
                    EmptyKit.closeAsyncQuietly(redisReplicator);
                }
            });
            redisReplicator.open();
        } catch (Exception e) {
            exceptionRef.set(e);
        }
        countDownLatch.countDown();
    }

    private void startCommandReplicatorThread(String table, RedisOffset redisOffset, CountDownLatch countDownLatch, HostAndPort node, Supplier<Boolean> isAlive) {
        try (
                Replicator redisReplicator = generateReplicator(redisOffset, node)
        ) {
            RedisUtil.dress(redisReplicator);
            AtomicReference<String> replId = new AtomicReference<>();
            AtomicLong offsetV1 = new AtomicLong();
            if (EmptyKit.isNotNull(redisOffset)) {
                replId.set(redisOffset.getReplId());
                offsetV1.set(redisOffset.getOffsetV1());
                replIdMap.put(replId.get(), node);
            }
            AtomicInteger dbNumber = new AtomicInteger();
            AtomicInteger helloCount = new AtomicInteger(0);
            redisReplicator.addEventListener((replicator, event) -> {
                if (!isAlive.get()) {
                    EmptyKit.closeAsyncQuietly(redisReplicator);
                    return;
                }
                try {
                    if (event instanceof DefaultCommand) {
                        DefaultCommand commandEvent = (DefaultCommand) event;
                        String command = new String(commandEvent.getCommand());
                        if ("SELECT".equals(command)) {
                            dbNumber.set(Integer.parseInt(new String(commandEvent.getArgs()[0])));
                            return;
                        }
                        if (((DefaultCommand) event).getArgs().length > 0) {
                            if (Arrays.equals(publishBytes, commandEvent.getCommand()) && Arrays.equals(helloBytes, ((DefaultCommand) event).getArgs()[0])) {
                                helloCount.incrementAndGet();
                                if (helloCount.get() >= 1000) {
                                    helloCount.set(0);
                                    enqueueEvent(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis()), new RedisOffset(replId.get(), offsetV1.get()));
                                }
                                return;
                            }
                        }
                        if (dbNumber.get() != redisConfig.getDatabase()) {
                            return;
                        }
                        Map<String, Object> after = TapSimplify.map(TapSimplify.entry("redis_key", commandEvent.getArgs().length > 0 ? new String(commandEvent.getArgs()[0]) : command));
                        if (Arrays.stream(commandEvent.getArgs()).map(v -> v.length).reduce(Integer::sum).orElse(0) > 10240) {
                            after.put("redis_value", "value too large, check info");
                        } else {
                            after.put("redis_value", command + "(" + Arrays.stream(commandEvent.getArgs()).map(String::new).collect(Collectors.joining(", ")) + ")");
                        }
                        TapRecordEvent recordEvent = new TapInsertRecordEvent().init().table(table).after(after).referenceTime(System.currentTimeMillis());
                        recordEvent.addInfo("redis_event", commandEvent);
                        offsetV1.set(commandEvent.getContext().getOffsets().getV2());
                        enqueueEvent(recordEvent, new RedisOffset(replId.get(), offsetV1.get()));
                    } else if (event instanceof AuxField) {
                        AuxField auxField = (AuxField) event;
                        if ("repl-id".equals(auxField.getAuxKey())) {
                            replId.set(auxField.getAuxValue());
                            replIdMap.put(replId.get(), node);
                        }
                    } else if (event instanceof PingCommand) {
                        enqueueEvent(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis()), new RedisOffset(replId.get(), offsetV1.get()));
                    }
                } catch (Exception e) {
                    exceptionRef.set(e);
                    EmptyKit.closeAsyncQuietly(redisReplicator);
                }
            });
            redisReplicator.open();
        } catch (Exception e) {
            exceptionRef.set(e);
        }
        countDownLatch.countDown();
    }

    public void readRdb(TapConnectorContext tapConnectorContext, String table, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, Supplier<Boolean> isAlive) throws Throwable {
        CountDownLatch countDownLatch;
        if (EmptyKit.isEmpty(clusterNodes)) {
            countDownLatch = new CountDownLatch(1);
            executorService.submit(() -> startRdbReplicatorThread(table, countDownLatch, new HostAndPort("0.0.0.0", 0), isAlive));
        } else {
            countDownLatch = new CountDownLatch(clusterNodes.size());
            for (ClusterNode node : clusterNodes) {
                executorService.submit(() -> startRdbReplicatorThread(table, countDownLatch, node.getMaster(), isAlive));
            }
        }
        boolean isFinish = false;
        List<TapEvent> eventList = TapSimplify.list();
        OffsetRedisEvent event;
        while (isAlive.get()) {
            if (EmptyKit.isNotNull(exceptionRef.get())) {
                throw exceptionRef.get();
            }
            event = eventQueue.poll(1, TimeUnit.SECONDS);
            if (event == null) {
                if (isFinish) {
                    break;
                }
                if (countDownLatch.await(500, TimeUnit.MILLISECONDS)) {
                    isFinish = true;
                }
            } else if (event.getEvent() instanceof StopEvent) {
                offsetMap.put(replIdMap.get(event.getOffset().getReplId()), event.getOffset());
            } else {
                eventList.add(event.getEvent());
                if (eventList.size() >= eventBatchSize) {
                    eventsOffsetConsumer.accept(eventList, new HashMap<>());
                    eventList.clear();
                }
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            eventsOffsetConsumer.accept(eventList, new HashMap<>());
        }
        tapConnectorContext.getStateMap().put("redisOffset", toJson(offsetMap));
    }

    public void readCommand(Map<HostAndPort, RedisOffset> offset, int recordSize, StreamReadConsumer consumer, Supplier<Boolean> isAlive) throws Throwable {
//        this.offsetMap = offset;
        offset.forEach((k, v) -> clusterNodes.stream().filter(node -> node.getMaster().equals(k) || (EmptyKit.isNotEmpty(node.getSlaves()) && node.getSlaves().contains(k))).findFirst().ifPresent(node -> offsetMap.put(node.getMaster(), v)));
        CountDownLatch countDownLatch;
        if (EmptyKit.isEmpty(clusterNodes)) {
            countDownLatch = new CountDownLatch(1);
            executorService.submit(() -> startCommandReplicatorThread(INIT_TABLE_NAME, offset.get(new HostAndPort("0.0.0.0", 0)), countDownLatch, new HostAndPort("0.0.0.0", 0), isAlive));
        } else {
            countDownLatch = new CountDownLatch(clusterNodes.size());
            for (ClusterNode node : clusterNodes) {
                executorService.submit(() -> startCommandReplicatorThread(INIT_TABLE_NAME, offset.get(node.getMaster()), countDownLatch, node.getMaster(), isAlive));
            }
        }
        boolean isFinish = false;
        List<TapEvent> eventList = TapSimplify.list();
        OffsetRedisEvent event;
        while (isAlive.get()) {
            if (EmptyKit.isNotNull(exceptionRef.get())) {
                throw exceptionRef.get();
            }
            event = eventQueue.poll(1, TimeUnit.SECONDS);
            if (event == null) {
                if (isFinish) {
                    break;
                }
                if (countDownLatch.await(500, TimeUnit.MILLISECONDS)) {
                    isFinish = true;
                }
            } else {
                offsetMap.put(replIdMap.get(event.getOffset().getReplId()), event.getOffset());
                eventList.add(event.getEvent());
                if (eventList.size() >= recordSize) {
                    consumer.accept(eventList, offsetMap);
                    eventList.clear();
                }
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            consumer.accept(eventList, offsetMap);
        }
    }

    private Replicator generateReplicator(RedisOffset redisOffset, HostAndPort clusterNode) throws Exception {
        String offsetStr;
        if (EmptyKit.isNull(redisOffset)) {
            offsetStr = "";
        } else {
            offsetStr = "&replOffset=" + redisOffset.getOffsetV1() + "&replId=" + redisOffset.getReplId();
        }
        if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.STANDALONE) {
            return new RedisReplicator(redisConfig.getReplicatorUri() + offsetStr);
        } else if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.SENTINEL) {
            int slavePort = 40000 + connectorId.hashCode() % 10000 + (EmptyKit.isNull(clusterNode) ? 0 : clusterNode.getPort());
            String sentinelUrl = redisConfig.getReplicatorUri() + "&slavePort=" + slavePort;
            RedisSentinelURI redisSentinelURI = new RedisSentinelURI(sentinelUrl);
            final Configuration configuration = Configuration.defaultSetting();
            if (EmptyKit.isNotNull(redisOffset)) {
                configuration.setReplId(redisOffset.getReplId());
                configuration.setReplOffset(redisOffset.getOffsetV1());
            }
            configuration.setSlavePort(slavePort);
            if (StringUtils.isNotBlank(redisConfig.getPassword())) {
                configuration.setAuthPassword(redisConfig.getPassword());
            }
            configuration.setRetries(36000);
            configuration.setRetryTimeInterval(10);
            return new RedisSentinelReplicator(redisSentinelURI, configuration);
        } else {
            return new RedisReplicator(redisConfig.getReplicatorUri(clusterNode) + offsetStr);
        }
    }

    protected void enqueueEvent(TapEvent event, RedisOffset offset) {
        try {
            while (!eventQueue.offer(new OffsetRedisEvent(event, offset), 1, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException ignore) {
        }
    }

    private String dumpValueToJson(Object obj) {
        if (obj instanceof ByteArrayMap) {
            Map<String, String> map = new HashMap<>();
            ((ByteArrayMap) obj).forEach((k, v) -> map.put(new String(k), new String(v)));
            return toJson(map);
        } else if (obj instanceof ByteArrayList) {
            List<String> list = new ArrayList<>();
            ((ByteArrayList) obj).forEach(v -> list.add(new String(v)));
            return toJson(list);
        } else if (obj instanceof byte[]) {
            return new String((byte[]) obj);
        } else if (obj instanceof ByteArraySet) {
            Set<String> set = new HashSet<>();
            ((ByteArraySet) obj).forEach(v -> set.add(new String(v)));
            return toJson(set);
        } else if (obj instanceof LinkedHashSet) {
            Set<ZSetElement> zSet = new LinkedHashSet<>();
            ((LinkedHashSet) obj).forEach(v -> {
                ZSetEntry entry = (ZSetEntry) v;
                ZSetElement zSetElement = new ZSetElement();
                zSetElement.setElement(new String(entry.getElement()));
                zSetElement.setScore(entry.getScore());
                zSet.add(zSetElement);
            });
            return toJson(zSet);
        } else {
            return toJson(obj);
        }
    }

    @Override
    public void close() throws Exception {
        Optional.ofNullable(executorService).ifPresent(ExecutorService::shutdown);
    }

    private static class OffsetRedisEvent {

        private OffsetRedisEvent(TapEvent event, RedisOffset offset) {
            this.event = event;
            this.offset = offset;
        }

        private TapEvent event;
        private RedisOffset offset;

        public TapEvent getEvent() {
            return event;
        }

        public void setEvent(TapEvent event) {
            this.event = event;
        }

        public RedisOffset getOffset() {
            return offset;
        }

        public void setOffset(RedisOffset offset) {
            this.offset = offset;
        }
    }
}
