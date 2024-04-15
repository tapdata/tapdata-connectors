package io.tapdata.connector.kafka.MultiThreadUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface SampleUtils {

	static <T> Consumer<T> showCountsInterval(long interval, BiConsumer<Long, T> callback) {
		return new Consumer<T>() {
			long lastPrintTime = 0;
			final AtomicLong counts = new AtomicLong(0);

			@Override
			public void accept(T param) {
				counts.incrementAndGet();
				if (System.currentTimeMillis() - lastPrintTime > interval) {
					lastPrintTime = System.currentTimeMillis();
					callback.accept(counts.getAndSet(0), param);
				}
			}
		};
	}

	static String loadFile(String pathStr) throws IOException {
		Path path = Paths.get(pathStr);
		byte[] bytes = Files.readAllBytes(path);
		return new String(bytes);
	}

	static String getScriptContext() {
		StringBuilder buildInMethod = new StringBuilder();
		buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
		buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
		buildInMethod.append("var LinkedHashMap = Java.type(\"java.util.LinkedHashMap\");\n");
		buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
		buildInMethod.append("var Date = Java.type(\"java.util.Date\");\n");
		buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
		buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
		buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
		buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
		buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
		buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
		buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
		buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
		buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");
		buildInMethod.append("var CustomParseUtil = Java.type(\"com.tapdata.processor.CustomParseUtil\");\n");
		buildInMethod.append("var sleep = function(ms){\n" +
			"var Thread = Java.type(\"java.lang.Thread\");\n" +
			"Thread.sleep(ms);\n" +
			"}\n");
		buildInMethod.append("var applyCustomParse = function(record){return CustomParseUtil.applyCustomParse(record);};\n");
		return buildInMethod.toString();
	}

	static Map<String, Object> createRecord(String op) {
		Map<String, Object> record = new HashMap<>();

		Map<String, Object> header = new HashMap<>();
		header.put("mqOp", op);
		record.put("header", header);

		Map<String, Map<String, Object>> data = new HashMap<>();

		Map<String, Object> before = new HashMap<>();
		before.put("id", UUID.randomUUID().toString());
		before.put("ts", System.currentTimeMillis());
		data.put("before", before);

		Map<String, Object> after = new HashMap<>();
		after.put("id", UUID.randomUUID().toString());
		after.put("ts", System.currentTimeMillis());
		data.put("after", after);

		record.put("data", data);

		return record;
	}
}
