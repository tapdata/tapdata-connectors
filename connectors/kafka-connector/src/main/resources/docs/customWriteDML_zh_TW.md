## 自定義DML消息體使用說明

### 輸入參數說明

1. record 代表從源端產出的數據，其結構如下

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
    	'XID':'事务Id',	//事件產生的事務ID
    	'rowId':'记录行标识'	//事件產生的行標識
    },
}
```

2. op代表操作類型，一共有3種操作類型，分別爲insert、update、delete
3. conditionKeys爲主鍵字段名集合，集合中包含了所有的主鍵

### 返回值說明

1. 返回的值代表寫入Kafka消息體的結構
2. 返回值對象中可以添加header和data兩個屬性，其中header代表kafka消息中的header部分，data代表kafka消息中的body部分：

```
customMeesage:{
	//自定義Kafka中的header，header裏面爲一對對的鍵值對
	'header':{key:value}
	//自定義Kafka中的body結構
	'data':{
		'tableId':record.eventInfo.tableId,
		'after':record.data.after,
		'rid':record.eventInfo.rowId,
	}
}
```

如上述返回值將在kafka消息的header中添加key爲op，值爲dml的header。
並且在body中寫入tableId、after、rid三個字段,值爲record的表名稱，record修改後的數據，還有記錄的rowId

3. 若返回值爲空，則代表過濾這條數據

### 示例

1. 將dml事件中的XID、rowId、tableId、以及在腳本新增一個字段屬性以及DML的數據自定義寫進Kafka的Body中，示例代碼如下：

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
