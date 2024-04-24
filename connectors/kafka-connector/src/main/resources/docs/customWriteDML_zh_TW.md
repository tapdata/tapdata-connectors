## 自定義DML消息體使用說明

### 輸入參數說明

#### record 代表從源端產出的數據，其結構如下

```
record:{
    'data':{
    	//before代表舊數據，delete與update事件會帶有此屬性,它的值類型爲鍵值對
    	'before':{
    		key:value
    	},
    	//after代表修新數據，insert與update事件會帶有此屬性,它的值類型爲鍵值對
    	'after':{
    		key:value
    	}
    },
    'eventInfo':{
    	'tableId':'TableName',	//表名
    	'referenceTime':0,	//事件產生的時間單位爲毫秒，類型爲Long
    	'XID':'5.30.714',	//事件產生的事務ID
    	'rowId':'AADUj5AAMAA'	//事件產生的行標識
    },
}
```

* op代表操作類型，一共有3種操作類型，分別爲insert、update、delete
* conditionKeys爲主鍵字段名集合，集合中包含了所有的主鍵

### 返回值說明

1. 返回的值代表寫入Kafka消息體的結構
2. 返回值對象中可以添加header和data兩個屬性，其中header代表kafka消息中的header部分，data代表kafka消息中的body部分：

```
customMeesage:{
	//自定義Kafka中的header
	'header':{key:value}
	//自定義Kafka中的body結構
	'data':{key:value}
}
```

* 如上的返回值會對kafka的消息體進行自定義，header與data都是以鍵值對的方式添加
* 若返回值爲空，則代表過濾這條數據

### 示例

將dml事件中的XID、rowId、tableId、以及在腳本新增一個字段屬性以及DML的數據自定義寫進Kafka的Body中，示例代碼如下：

```
//創建一個LinkedHashMap對象作爲body容器對象
let body = new LinkedHashMap();
//往body裏面添加自定義屬性
body.put("XID",record.eventInfo.XID);
body.put("rowId",record.eventInfo.rowId);
body.put("tableId",record.eventInfo.tableId);
body.put("before",record.data.before);
body.put("after",record.data.after);
//創建一個LinkedHashMap對象作爲header容器對象
let header = new LinkedHashMap();
//往header裏面添加自定義header
header.put("op","dml");
//創建返回值對象，並將上面自定義好的body和header 添加到返回值對象中
let message = new LinkedHashMap();
message.put("data",body);
message.put("header",header);
return message;
```
