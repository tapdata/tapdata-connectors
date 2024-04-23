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
