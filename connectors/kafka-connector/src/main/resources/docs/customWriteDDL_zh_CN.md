## 自定义DDL消息体使用说明

### 输入参数说明

record代表从源端出来的DDL记录，其结构如下：

```
var record={
	'referenceTime':1713724313,	//事件产生的时间单位为毫秒，类型为Long
	'time':1713724313,	//事件产生的时间单位为毫秒，类型为Long
	'type':203,	//代表DDL的事件类型，209代表新增字段事件，201代表修改字段属性事件，202代表修改字段名事件，207代表删除字段事件
	'ddl':"alter table add column test varchar(1)",	//代表数据库原始的DDL语句
	'tableId':"test",		//代表发生DDL的表名称
	// 新增字段DDL事件特有属性
	'newFields':[
		{
			'name':'f1',		//代表新增的字段名
			'autoInc': false,		//代表是否为自增字段
			'dataType':'STRING',		//代表字段类型
			'nullable': true,		//代表是否可以为空
			'partitionKey':false,		//代表是否为分区字段
			'primaryKey':false		//代表是否为主键字段
		}
	]
	// 修改字段名DDL事件特有属性
	nameChange:{
		after:'改名後的值',
		before:'改名前的值'
	}
	// fieldName、checkChange、constraintChange、nullableChange、commentChange、defaultChange、primaryChange、fieldName是修改字段属性DDL事件特有属性
	fieldName:‘修改属性的字段名属性’
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// 字段check约束变化
	checkChange:{
		before:'',
		after:''
	},
	//字段constraint约束变化
	constraintChange:{
		before:'',
		after:''
	},
	//字段nullable约束变化
	nullableChange:{
		before:false,
		after:true
	},
	//字段comment约束变化
	commentChange:{
		before:'',
		after:''
	},
	//字段default约束变化
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue,
	},
	//字段primary约束变化
	primaryChange:{
		before:0
		after:1
	}
	// 删除字段DDL事件特有属性
	fieldName:'删除的字段名'
}
```
### 返回值说明
1. 返回值代表写入Kafka的body结构
2. 返回null代表过滤这条数据
### 常用方法
1. 如上示例将字段改名DDL事件自定义写进Kafka消息的body中，根据入参record的type判断是哪种DDL事件，然后添加opType，并且在body中写入表属主，表名，还有DDL的原始语句
```
	//创建一个LinkedHashMap对象作为body容器对象
	let data = new LinkedHashMap();

	switch (record.type) {
	   case 209:
		data.put("op", 2001); // 操作类型编码（数字）
		data.put("opType", "addField"); // 操作类型（字符串）
		break;
	   case 201:
	   case 202:
		data.put("op", 2002); // 操作类型编码（数字）
		data.put("opType", "changeField"); // 操作类型（字符串）
		break;
	   case 207:
		data.put("op", 2003); // 操作类型编码（数字）
		data.put("opType", "deleteField"); // 操作类型（字符串）
		break;
	   default:
		return null; // 不支持的 DDL 返回 null
	}

  	data.put("time", record.referenceTime); // DDL 发生时间戳（秒）
  	data.put("owner", "taptest"); // 表属主
  	data.put("name", record.tableId); // 表名
  	data.put("sql", record.ddl); // DDL原始语句
  	data.put("tapType", record); // Tapdata DDL 类型
  	return data;
```
