## 自定义DML消息体使用说明

### 输入参数说明

#### record 代表从源端产出的数据，其结构如下:
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
    	'XID':'5.30.714',	//事件产生的事务ID
    	'rowId':'AADUj5AAMAA'	//事件产生的行标识
    },
}
```
* op代表操作类型，一共有3种操作类型，分别为insert、update、delete
* conditionKeys为主键字段名集合，集合中包含了所有的主键

### 返回值说明

* 返回的值代表写入Kafka消息体的结构
* 返回值对象中可以添加header和data两个属性，其中header代表kafka消息中的header部分，data代表kafka消息中的body部分

```
customMeesage:{
	//自定义Kafka中的header
	'header':{key:value},
	//自定义Kafka中的body结构
	'data':{key:value}
}
```

* 如上的返回值会对kafka的消息体进行自定义，header与data都是以键值对的方式添加
* 若返回值为空，则代表过滤这条数据

### 示例

将dml事件中的XID、rowId、tableId、以及在脚本新增一个tapd字段以及DML的数据自定义写进Kafka的Body中，示例代码如下：

```
//创建一个LinkedHashMap对象作为body容器对象
let body = new LinkedHashMap();
//往body里面添加自定义属性
body.put("XID",record.eventInfo.XID);
body.put("rowId",record.eventInfo.rowId);
body.put("tableId",record.eventInfo.tableId);
body.put("before",record.data.before);
body.put("after",record.data.after);
body.put("tapd","tapdata");
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
