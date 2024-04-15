package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;

import javax.script.ScriptEngine;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/15 14:52 Create
 */
public class ConcurrentScriptEngine extends Concurrents<ScriptEngine> {

	@Override
	protected String getInstanceId() {
		return String.format("hashcode-%d", Thread.currentThread().getId());
	}

	@Override
	protected ScriptEngine initNewInstance(String instanceId) {
		try {
			ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
			String script = SampleUtils.loadFile("/Users/lhs/IdeaProjects/tapd8-ws/v3-change/tapdata/iengine/iengine-app/src/main/java/io/tapdata/concurrent/test.js");

			// 初始化引擎
			ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("graal.js"));
			String buildInMethod = SampleUtils.getScriptContext();
			String scripts = script + System.lineSeparator() + buildInMethod;
			scriptEngine.eval(scripts);
			return scriptEngine;
		} catch (Exception e) {
			throw new RuntimeException("Init script engine failed: " + e.getMessage(), e);
		}
	}
}
