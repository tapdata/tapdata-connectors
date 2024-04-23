## 自定義DDL消息體使用說明

### 輸入參數說明

record代表從源端出來的DDL記錄，其結構如下
```
var record={
	'referenceTime':1713724313,	//事件產生的時間單位爲毫秒，類型爲Long
	'time':1713724313,	//事件產生的時間單位爲毫秒，類型爲Long
	'type':203,	//代表DDL的事件類型，209代表新增字段事件，201代表修改字段屬性事件，202代表修改字段名事件，207代表刪除字段事件
	'ddl':"alter table add column test varchar(1)",	//代表數據庫原始的DDL語句
	'tableId':"test",		//代表發生DDL的表名稱
	// 新增字段DDL事件特有屬性
	'newFields':[
		{
			'name':'f1',		//代表新增的字段名
			'autoInc': false,		//代表是否爲自增字段
			'dataType':'STRING',		//代表字段類型
			'nullable': true,		//代表是否可以爲空
			'partitionKey':false,		//代表是否爲分區字段
			'primaryKey':false		//代表是否爲主鍵字段
		}
	]
	// 修改字段名DDL事件特有屬性
	nameChange:{
		after:'改名後的值',
		before:'改名前的值'
	}
	// fieldName、checkChange、constraintChange、nullableChange、commentChange、defaultChange、primaryChange、fieldName是修改字段屬性DDL事件特有屬性
	fieldName:'修改屬性的字段名屬性'
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// 字段check約束變化
	checkChange:{
		before:'',
		after:''
	},
	//字段constraint約束變化
	constraintChange:{
		before:'',
		after:''
	},
	//字段nullable約束變化
	nullableChange:{
		before:false,
		after:true
	},
	//字段comment約束變化
	commentChange:{
		before:'',
		after:''
	},
	//字段default約束變化
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue,
	},
	//字段primary约束变化
	primaryChange:{
		before:0
		after:1
	}
	// 刪除字段DDL事件特有屬性
	fieldName:'刪除的字段名'
}
```
### 返回值說明
1. 返回值代表寫入Kafka的body結構
2. 返回null代表過濾這條數據
### 示例
1. 如上示例將字段改名DDL事件自定義寫進Kafka消息的body中，根據入參record的type判斷是哪種DDL事件，然後添加opType，並且在body中寫入表屬主，表名，還有DDL的原始語句
```
	//創建一個LinkedHashMap對象作爲body容器對象
	let data = new LinkedHashMap();

	switch (record.type) {
	   case 209:
		data.put("op", 2001); // 操作類型編碼（數字）
		data.put("opType", "addField"); // 操作類型（字符串）
		break;
	   case 201:
	   case 202:
		data.put("op", 2002); // 操作類型編碼（數字）
		data.put("opType", "changeField"); // 操作類型（字符串）
		break;
	   case 207:
		data.put("op", 2003); // 操作類型編碼（數字）
		data.put("opType", "deleteField"); // 操作類型（字符串）
		break;
	   default:
		return null; // 不支持的 DDL 返回 null
	}

  	data.put("time", record.referenceTime); // DDL 發生時間戳（秒）
  	data.put("owner", "taptest"); // 表屬主
  	data.put("name", record.tableId); // 表名
  	data.put("sql", record.ddl); // DDL原始語句
  	data.put("tapType", record); // Tapdata DDL 類型
  	return data;
```
