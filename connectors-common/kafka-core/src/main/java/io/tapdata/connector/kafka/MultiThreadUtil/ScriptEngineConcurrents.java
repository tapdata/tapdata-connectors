package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.connector.kafka.util.ScriptUtil;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.utils.InstanceFactory;

import javax.script.Invocable;

public class ScriptEngineConcurrents<T> extends Concurrents<Invocable> {
	private ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");

	private String script;
	public static final String scriptEngineName = "graal.js";

	public ScriptEngineConcurrents(String script) {
		this.script = script;
	}

	@Override
	protected String getInstanceId() {
		String id = String.valueOf(Thread.currentThread().getId());
		return id;
	}

	@Override
	protected Invocable initNewInstance(String instanceId) {
		Invocable customParseEngine = ScriptUtil.getScriptEngine(script, scriptEngineName, scriptFactory);
		return customParseEngine;
	}
}
