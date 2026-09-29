package io.tapdata.mongodb;

import io.tapdata.mongodb.decoder.CustomDocument;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read-only find query parsed from a runRawCommand command.
 * <p>
 * The command is either a find filter, or {@code {"filter":{},"sort":{},"limit":10}}. It is treated as the wrapped
 * form only when every top-level key is one of {@code filter/sort/limit}, so a filter on a field that happens to be
 * named {@code limit} still works when combined with other fields.
 */
public class MongodbRawCommandQuery {

	static final int DEFAULT_LIMIT = 100;

	private static final Set<String> WRAPPED_KEYS = new HashSet<>(Arrays.asList("filter", "sort", "limit"));

	private static final String EXAMPLE = "{\"filter\":{},\"sort\":{},\"limit\":10}";

	private final Document filter;
	private final Document sort;
	private final int limit;

	private MongodbRawCommandQuery(Document filter, Document sort, int limit) {
		this.filter = filter;
		this.sort = sort;
		this.limit = limit;
	}

	/**
	 * @param command raw command text
	 * @param maxRows upper bound of returned rows; the requested limit never exceeds it
	 */
	public static MongodbRawCommandQuery parse(String command, int maxRows) {
		int max = maxRows > 0 ? maxRows : DEFAULT_LIMIT;
		Document document = parseDocument("command", command);
		if (!isWrapped(document)) {
			return new MongodbRawCommandQuery(document, new Document(), max);
		}
		return new MongodbRawCommandQuery(
				asDocument("filter", document.get("filter")),
				asDocument("sort", document.get("sort")),
				resolveLimit(document.get("limit"), max));
	}

	private static boolean isWrapped(Document document) {
		return !document.isEmpty() && WRAPPED_KEYS.containsAll(document.keySet());
	}

	private static Document parseDocument(String name, String json) {
		try {
			return CustomDocument.parse(json);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("MongoDB raw command " + name + " must be a JSON object, for example " + EXAMPLE + ": " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private static Document asDocument(String name, Object value) {
		if (value == null) {
			return new Document();
		}
		if (value instanceof Document) {
			return (Document) value;
		}
		if (value instanceof Map) {
			return new Document((Map<String, Object>) value);
		}
		if (value instanceof String) {
			return parseDocument(name, (String) value);
		}
		throw new IllegalArgumentException("MongoDB raw command field '" + name + "' must be a JSON object, but got: " + value);
	}

	private static int resolveLimit(Object value, int max) {
		if (value instanceof Number && ((Number) value).longValue() > 0) {
			return (int) Math.min(((Number) value).longValue(), max);
		}
		return max;
	}

	public Document getFilter() {
		return filter;
	}

	public Document getSort() {
		return sort;
	}

	public int getLimit() {
		return limit;
	}
}
