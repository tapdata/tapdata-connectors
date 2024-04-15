package io.tapdata.connector.kafka.MultiThreadUtil;

import javax.script.Invocable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 并发处理工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/15 11:39 Create
 */
public abstract class Concurrents<T> implements AutoCloseable {

	protected final AtomicBoolean isClosed = new AtomicBoolean(false);
	protected final Map<String, T> instanceMap = new HashMap<>();

	protected abstract String getInstanceId();

	protected abstract T initNewInstance(String instanceId);

	public <V> V process(Function<T, V> fn) {
		if (isClosed.get()) throw new RuntimeException("Already closed");

		String instanceId = getInstanceId();
		T instance = instanceMap.get(instanceId);
		if (null == instance) {
			synchronized (instanceMap) {
				instance = instanceMap.get(instanceId);
				if (null == instance) {
					instance = initNewInstance(instanceId);
					instanceMap.put(instanceId, instance);
				}
			}
		}
		return fn.apply(instance);
	}

	@Override
	public void close() throws Exception {
		if (isClosed.compareAndSet(false, true)) {
			AtomicReference<Exception> error = new AtomicReference<>(null);
			instanceMap.values().forEach(t -> {
				try {
					if (t instanceof AutoCloseable) {
						((AutoCloseable) t).close();
					}
				} catch (Exception e) {
					if (!error.compareAndSet(null, e)) {
						error.get().addSuppressed(e);
					}
				}
			});
			instanceMap.clear();

			if (null != error.get()) throw error.get();
		}
	}

	public static void main(String[] args) {
		// 每条记录都创建 js 引擎
		// duration: PT1M59.207S counts:100000, qps:832.3761008173933
		// 引擎单独初始化
		// duration: PT1.801S counts:100000, qps:28686.173264486515
		// duration: PT7.239S counts:500000, qps:57319.729450876985
		// duration: PT21.533S counts:1000000, qps:43432.939541348154
		// 增加 200 行 “data.put("sdbType", "mysql"); // 数据库类型，如：oracle, db2, tdsql, tidb”
		// duration: PT1M17.811S counts:1000000, qps:12608.273549102922

		// 1 并发 100/批
		// duration: PT23.471S counts:1000000, qps:42603.95364689843
		// 4 并发 100/批
		// duration: PT12.064S counts:1000000, qps:82877.5070445881
		// 8 并发 100/批
		// duration: PT13.581S counts:1000000, qps:73621.43856290952
		// 8 并发 1000/批
		// duration: PT12.613S counts:1000000, qps:79270.70947284978
		// 8 并发 64/批
		// duration: PT13.992S counts:1000000, qps:71464.30358036161
		// 没有解析，8 并发 64/批
		// duration: PT2.782S counts:1000000, qps:359195.4022988506
		// js 中直接返回结果，8 并发 64/批
		// duration: PT5.448S counts:1000000, qps:183519.9119104423
		// batch 100:
		// - 16c duration: PT9.122S counts:1000000, qps:109601.05217010083
		// -  8c duration: PT8.597S counts:1000000, qps:116279.06976744186
		// - 10c duration: PT8.739S counts:1000000, qps:114416.47597254004
		// -  1c duration: PT34.352S counts:1000000, qps:29109.539195994526
		// -  2c duration: PT18.99S counts:1000000, qps:52650.976675617334
		// batch 1000:
		// - 10c duration: PT7.835S counts:1000000, qps:127616.1306789178
		// - 10c duration: PT8.379S counts:1000000, qps:119317.50387781888

		try (
			ConcurrentScriptEngine concurrentScriptEngine = new ConcurrentScriptEngine();
			ShowQps<Object> showCountsInterval = new ShowQps<>(1000, (parent, param) -> System.out.println(parent));
			ConcurrentCalculator<Object,Object> calculator = new ConcurrentCalculator<Object,Object>(10) {
				@Override
				protected Object performComputation(Object record) {
					return concurrentScriptEngine.process(scriptEngine -> {
						try {
							HashMap<String, Object> context = new HashMap<>();
							scriptEngine.put("context", context);

							String op = "insert";
							List<String> keys = new ArrayList<>();

							// 转换数据
							Invocable invocable = (Invocable) scriptEngine;
							return invocable.invokeFunction("process", record, op, keys);
						} catch (Exception e) {
							throw new RuntimeException("JS invoke failed: " + e.getMessage(), e);
						}
					});
				}
			}
		) {
			int counts = 1000000;
			int batchSize = 100;
			long begin = System.currentTimeMillis();
			List<Object> dataList = new ArrayList<>();
			for (int i = 0; i < counts; i++) {

				Map<String, Object> record = SampleUtils.createRecord("insert");
				dataList.add(record);

				if (i != 0 && i % batchSize == 0) {
					calculator.calc(dataList, showCountsInterval::process);
					dataList.clear();
				}
			}

			if (!dataList.isEmpty()) {
				calculator.calc(dataList, showCountsInterval::process);
				dataList.clear();
			}

			System.out.println("duration: " + Duration.ofMillis(System.currentTimeMillis() - begin));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
