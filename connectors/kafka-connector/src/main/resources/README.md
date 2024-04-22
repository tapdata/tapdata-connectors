## 自定义DML消息体使用说明

### 输入参数说明

1. record 代表从源端产出的数据，其结构如下

```
record:{
    'data':{
    	//before代表旧数据，delete与update事件会带有此属性,它的值类型为键值对
    	'before':{
    		key:value
    	},
    	//after代表修新数据，insert与update事件会带有此属性,它的值类型为键值对
    	'after':{
    		key:value
    	}
    },
    'eventInfo':{
    	'tableId':'TableName',	//表名
    	'referenceTime':0,	//事件产生的时间单位为毫秒，类型为Long
    	'XID':'事务Id',	//事件产生的事务ID
    	'rowId':'记录行标识'	//事件产生的行标识
    },
}
```

2. op代表操作类型，一共有3种操作类型，分别为insert、update、delete
3. conditionKeys为主键字段名集合，集合中包含了所有的主键

### 返回值说明

1. 返回的值代表写入Kafka消息体的结构
2. 返回值对象中可以添加header和data两个属性，其中header代表kafka消息中的header部分，data代表kafka消息中的body部分：

```
customMeesage:{
	//自定义Kafka中的header，header里面为一对对的键值对
	'header':{key:value}
	//自定义Kafka中的body结构
	'data':{
		'tableId':record.eventInfo.tableId,
		'after':record.data.after,
		'rid':record.eventInfo.rowId,
	}
}
```

如上述返回值将在kafka消息的header中添加key为op，值为dml的header。
并且在body中写入tableId、after、rid三个字段,值为record的表名称，record修改后的数据，还有记录的rowId

3. 若返回值为空，则代表过滤这条数据

### 示例

1. 将dml事件中的XID、rowId、tableId、以及在脚本新增一个字段属性以及DML的数据自定义写进Kafka的Body中，示例代码如下：

```
//创建一个LinkedHashMap对象作为body容器对象
let body = new LinkedHashMap();
//往body里面添加自定义属性
body.put("XID",record.eventInfo.XID);
body.put("rowId",record.eventInfo.rowId);
body.put("tableId",record.eventInfo.tableId);
body.put("before",record.data.before);
body.put("after",record.data.after);
//创建一个LinkedHashMap对象作为header容器对象
let header = new LinkedHashMap();
//往header里面添加自定义header
header.put("op","dml");
//创建返回值对象，并将上面自定义好的body和header 添加到返回值对象中
let message = new LinkedHashMap();
message.put("data",body);
message.put("header",header);
return message;
```

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
## 自定义解析Kafka消息体使用说明
### 输入参数说明
record为每条消息的body 要将每条消息转换为Tapdata 的DML或DDL事件，需要遵守以下协议。
1. 若要转为添加字段DDL事件,脚本的返回值需要遵守以下协议:
```
{type:209, newFields=[{name:'fieldName',dataType:'fieldDataType'}]}
```
newFields是一个数组。代表所有新增的字段。dateType的值可以为Map、Array、Number、Boolean、String、Integer、Text。
2. 若要转为改名DDL,脚本的返回值需要遵守以下协议:
```
{type=202, nameChange={after:'afterName',before:'beforeName'}}
```
nameChange代表修改前后的字段名，这个值一定不能为空，否则会转换不成功，报错。
3. 若要转为删除字段DDL，脚本返回值的对象需要有以下协议:
```
{type:207，fieldName:'deleteFieldName'}
```
fieldName代表要删除的字段名，这个值一定不能为空，否则会转换不成功，报错。
4. 若要转为修改字段属性DDL，脚本的返回值需要遵守以下协议:
```
{
	type:201,
	fieldName:'fieldName'
	// dataType代表字段类型的变化
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// checkChange代表字段check约束的变化
	checkChange:{
		before:'',
		after:''
	},
	// constraintChange代表字段constraint约束的变化
	constraintChange:{
		before:'',
		after:''
	},
	// nullableChange代表字段nullable约束的变化
	nullableChange:{
		before:false,
		after:true
	},
	// commentChange代表字段comment约束的变化
	commentChange:{
		before:'',
		after:''
	},
	// defaultChange代表字段default约束的变化
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue,
	},
	// primaryChange代表修改字段primary约束的变化
	primaryChange:{
		before:0
		after:1
	}
}
```
其中fieldName代表需要修改属性的列名,fieldName不能为空，否则会报错。<br/>
dataTypeChange、checkChange、constraintChange、nullableChange、commentChange、defaultChange、primaryChange这其中有一个一定不能为空，否则会报错。
4. 若要转为dml事件，脚本的返回值需要有以下协议:
```
{
	type:'insert或者'update或者'delete',
	after:{} //insert和update不能为空,
	before:{} //delete事件不能为null
}
```
type的值可以为insert、update、delete，其中当type为insert和update时，after不能为空，当type为delete时，before不能为空,否则会报错。
5. 若要丢弃此消息，则return null
### 示例
1. 如下示例将op值为1001、1002、1003转为DML事件，，op值为2001、2002、2003转为DDL事件。
```
let data = new LinkedHashMap();
switch (record.op) {
   case 1001:
   case 1002:
   case 1003:
    data.put("op", record.optype);
    data.put("before", record.before);
    data.put("after", record.after);
    return data;
   case 2001:
   case 2002:
   case 2003:
    data.put("op", "ddl");
    data.putAll(record.tapType);
    return data;
   default:
    return null;
}
```

