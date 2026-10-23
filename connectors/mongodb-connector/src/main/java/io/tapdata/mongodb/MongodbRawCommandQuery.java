package io.tapdata.mongodb;

import io.tapdata.mongodb.decoder.CustomDocument;
import org.bson.Document;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read-only find query parsed from a runRawCommand command.
 * <p>
 * Two forms are accepted, told apart without guessing:
 * <ul>
 *     <li>a plain find filter, e.g. {@code {"status":"PAID"}}. Every top-level key is taken literally, so a filter
 *     on a field named {@code filter}, {@code sort} or {@code limit} queries that field;</li>
 *     <li>the wrapped form {@code {"$find":{"filter":{},"sort":{},"limit":10}}}, which also carries sort and limit.
 *     MongoDB forbids {@code $} prefixed field names and has no {@code $find} query operator, so the marker can
 *     never collide with a filter.</li>
 * </ul>
 * A blank command or {@code {}} means "match everything", sampled up to the caller's row limit. Server side
 * JavaScript operators are rejected, since the command comes straight from user input.
 */
public class MongodbRawCommandQuery {

	static final String WRAPPER_KEY = "$find";

	static final int DEFAULT_LIMIT = 100;

	private static final String FILTER_KEY = "filter";

	private static final String SORT_KEY = "sort";

	private static final String LIMIT_KEY = "limit";

	private static final Set<String> WRAPPED_KEYS = new HashSet<>(Arrays.asList(FILTER_KEY, SORT_KEY, LIMIT_KEY));

	/**
	 * Operators that evaluate JavaScript on the server, see
	 * <a href="https://www.mongodb.com/docs/manual/reference/operator/query/where/">$where</a>.
	 */
	private static final Set<String> JAVASCRIPT_OPERATORS = new HashSet<>(Arrays.asList("$where", "$function", "$accumulator"));

	private static final String EXAMPLE = "{\"$find\":{\"filter\":{},\"sort\":{},\"limit\":10}}";

	private final Document filter;
	private final Document sort;
	private final int limit;

	private MongodbRawCommandQuery(Document filter, Document sort, int limit) {
		this.filter = filter;
		this.sort = sort;
		this.limit = limit;
	}

	/**
	 * @param command      raw command text, either a find filter or the {@code $find} wrapped form
	 * @param defaultLimit row limit used when the command does not carry one of its own
	 */
	public static MongodbRawCommandQuery parse(String command, int defaultLimit) {
		int fallbackLimit = defaultLimit > 0 ? defaultLimit : DEFAULT_LIMIT;
		Document document = parseDocument("command", command);
		MongodbRawCommandQuery query = document.containsKey(WRAPPER_KEY)
				? parseWrapped(document, fallbackLimit)
				: new MongodbRawCommandQuery(document, new Document(), fallbackLimit);
		rejectJavaScriptOperators(query.filter);
		rejectJavaScriptOperators(query.sort);
		return query;
	}

	private static MongodbRawCommandQuery parseWrapped(Document document, int fallbackLimit) {
		if (document.size() > 1) {
			throw new IllegalArgumentException("MongoDB raw command must not mix '" + WRAPPER_KEY
					+ "' with other keys, for example " + EXAMPLE + ", but got keys: " + document.keySet());
		}
		Document wrapped = asDocument(WRAPPER_KEY, document.get(WRAPPER_KEY));
		Set<String> unknownKeys = new HashSet<>(wrapped.keySet());
		unknownKeys.removeAll(WRAPPED_KEYS);
		if (!unknownKeys.isEmpty()) {
			throw new IllegalArgumentException("MongoDB raw command '" + WRAPPER_KEY + "' only supports "
					+ WRAPPED_KEYS + ", for example " + EXAMPLE + ", but got: " + unknownKeys);
		}
		return new MongodbRawCommandQuery(
				asDocument(FILTER_KEY, wrapped.get(FILTER_KEY)),
				asDocument(SORT_KEY, wrapped.get(SORT_KEY)),
				resolveLimit(wrapped.get(LIMIT_KEY), fallbackLimit));
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

	private static int resolveLimit(Object value, int fallbackLimit) {
		if (value == null) {
			return fallbackLimit;
		}
		if (!(value instanceof Number) || ((Number) value).longValue() <= 0) {
			throw new IllegalArgumentException("MongoDB raw command field '" + LIMIT_KEY + "' must be a positive number, but got: " + value);
		}
		return (int) Math.min(((Number) value).longValue(), Integer.MAX_VALUE);
	}

	private static void rejectJavaScriptOperators(Object value) {
		if (value instanceof Map) {
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
				if (JAVASCRIPT_OPERATORS.contains(entry.getKey())) {
					throw new IllegalArgumentException("MongoDB raw command must not use server side JavaScript operator '"
							+ entry.getKey() + "', supported operators are query operators only");
				}
				rejectJavaScriptOperators(entry.getValue());
			}
		} else if (value instanceof Collection) {
			for (Object element : (Collection<?>) value) {
				rejectJavaScriptOperators(element);
			}
		}
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
