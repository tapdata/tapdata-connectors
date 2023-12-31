package io.tapdata.partition.error;

/**
 * @author aplomb
 */
public interface ConnectorErrors {

	int MISSING_COUNT_BY_PARTITION_FILTER = 13001;
	int MISSING_QUERY_FIELD_MIN_MAX_VALUE = 13002;
	int MISSING_TABLE = 13003;
	int MISSING_CONSUMER = 13004;
	int MISSING_CONNECTOR_CONTEXT = 13005;
	int MISSING_TYPE_SPLITTER = 13006;
	int MIN_MAX_CANNOT_CONVERT_TO_DATETIME = 13007;
}
