function process(record, op, conditionKeys) {
	let data = new LinkedHashMap();

	switch (op) {
		case "insert":
			data.put("op", 1001); // 操作类型编码（数字）
			data.put("optype", op); // 操作类型（字段串）
			break;
		case "update":
			data.put("op", 1002);
			data.put("optype", op); // 操作类型（字段串）
			break;
		case "delete":
			data.put("op", 1003);
			data.put("optype", op); // 操作类型（字段串）
			break;
		default:
			return null;
	}

	data.put("sdbType", "mysql"); // 数据库类型，如：oracle, db2, tdsql, tidb
	data.put("owner", "taptest"); // 表的属主
	data.put("time", context.referenceTime); // DML 发生时间戳（秒）
	data.put("name", context.tableId); // 表名

	if (record.data) {
		data.put("before", record.data.before); // 新数据值
		data.put("after", record.data.after); // 旧数据值
	}

	data.put("xid", "-"); // 事务 ID
	data.put("scn", "-"); // 扩展，DML操作序号，如：oracle.SCN, db2.LSN
	data.put("cmttime", "-"); // DML 事务提交时间戳（秒）
	data.put("rowid", "-"); // 记录行标识
	data.put("pkname", conditionKeys); // 主键列名称

	record.data = data;
	return record;
}
