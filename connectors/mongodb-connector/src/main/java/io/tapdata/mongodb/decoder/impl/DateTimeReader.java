package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.mongodb.decoder.CustomSQLObject;
import io.tapdata.util.DateUtil;
import org.bson.Document;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * @author samuel
 * @Description
 * @create 2024-05-31 17:59
 **/
public class DateTimeReader implements CustomSQLObject<Object, Void> {

	public static final String DATE_TIME_KEY = "$dateTime";
	public final ClassHandlers classHandlers;

	public DateTimeReader() {
		this.classHandlers = new ClassHandlers();
		classHandlers.register(Document.class, this::handleDocument);
		classHandlers.register(String.class, this::handleString);
	}

	@Override
	public Object execute(Object functionObj, Void curMap) {
		return classHandlers.handle(functionObj);
	}

	private Object handleString(String value) {
		String format = DateUtil.determineDateFormat(value);
		if (null != format) {
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format).withZone(ZoneId.of("GMT"));
			TemporalAccessor temporalAccessor = dateTimeFormatter.parse(value);
			return Instant.from(temporalAccessor);
		}
		return value;
	}

	private Object handleDocument(Document value) {
		try {
			DateTime dateTime = TapSimplify.fromJson(value.toJson(), DateTime.class);
			return dateTime.toInstant();
		} catch (Exception ignored) {
			// do nothing
		}
		return value;
	}

	@Override
	public String getFunctionName() {
		return DATE_TIME_KEY;
	}
}
