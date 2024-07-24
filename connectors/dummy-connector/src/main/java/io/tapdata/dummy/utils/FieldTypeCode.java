package io.tapdata.dummy.utils;

import io.tapdata.entity.schema.TapField;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author samuel
 * @Description
 * @create 2024-07-04 17:09
 **/
public class FieldTypeCode {
	TapField tapField;
	int type;
	Integer length;
	AtomicLong serial;
	Integer step;

	public FieldTypeCode(TapField tapField, int type) {
		this.tapField = tapField;
		this.type = type;
	}

	public TapField getTapField() {
		return tapField;
	}

	public int getType() {
		return type;
	}

	public Integer getLength() {
		return length;
	}

	public void setLength(Integer length) {
		this.length = length;
	}

	public AtomicLong getSerial() {
		return serial;
	}

	public void setSerial(AtomicLong serial) {
		this.serial = serial;
	}

	public Integer getStep() {
		return step;
	}

	public void setStep(Integer step) {
		this.step = step;
	}
}
