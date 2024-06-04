package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.mongodb.decoder.CustomSQLObject;
import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2024-05-31 17:59
 **/
public class DateTimeReader implements CustomSQLObject<Object, Void> {

	public static final String DATE_TIME_KEY = "$dateTime";

	@Override
	public Object execute(Object functionObj, Void curMap) {
		if (functionObj instanceof Document) {
			try {
				DateTime dateTime = TapSimplify.fromJson(((Document) functionObj).toJson(), DateTime.class);
				return dateTime.toInstant();
			} catch (Exception e) {
				// do nothing
			}
		}
		return functionObj;
	}

	@Override
	public String getFunctionName() {
		return DATE_TIME_KEY;
	}
}
